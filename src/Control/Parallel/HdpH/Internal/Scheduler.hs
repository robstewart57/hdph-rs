
-- Work stealing scheduler and thread pools
--
-- Visibility: HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 06 Jul 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- req'd for type 'RTS'
{-# LANGUAGE ScopedTypeVariables #-}         -- req'd for type annotations
{-# LANGUAGE FlexibleContexts #-}         -- req'd for type annotations

module Control.Parallel.HdpH.Internal.Scheduler
  ( -- * abstract run-time system monad
    RTS,          -- instances: Monad, Functor
    run_,         -- :: RTSConf -> RTS () -> IO ()
    liftThreadM,  -- :: ThreadM RTS a -> RTS a
    liftSparkM,   -- :: SparkM RTS a -> RTS a
    liftCommM,    -- :: CommM a -> RTS a
    liftIO,       -- :: IO a -> RTS a

    -- * converting and executing threads
    mkThread,     -- :: ParM RTS a -> Thread RTS
    execThread,   -- :: Thread RTS -> RTS ()

    -- * pushing sparks and values
    sendPUSH      -- :: Spark RTS -> NodeId -> RTS ()

  ) where

import Prelude hiding (error)
import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Monad (unless,void,foldM,when,replicateM_,replicateM_)
import Data.Functor ((<$>))
import Data.Maybe (fromJust,isJust)
import Control.Parallel.HdpH.Closure (unClosure,toClosure)
import Control.Parallel.HdpH.Conf (RTSConf(scheds, wakeupDly))
import Control.Parallel.HdpH.Internal.Comm (CommM)
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (myNode,rmNode,send, receive, run_, waitShutdown)
import qualified Control.Parallel.HdpH.Internal.Data.Deque as Deque (emptyIO)
import qualified Control.Parallel.HdpH.Internal.Data.Sem as Sem (new, signalPeriodically)
import qualified Control.Parallel.HdpH.Internal.Location
         as Location (debug)
import Control.Parallel.HdpH.Internal.Location
         (dbgStats, dbgMsgSend, dbgMsgRcvd, dbgFailure, error, NodeId)
import Control.Parallel.HdpH.Internal.Misc
       (encodeLazy, decodeLazy, ActionServer, newServer, killServer)
import Control.Parallel.HdpH.Internal.Sparkpool
       (SparkM, blockSched, putSpark, getSpark,popGuardPostToSparkpool,
        dispatch, readPoolSize,readFishSentCtr, readSparkRcvdCtr,
        readSparkGenCtr, readMaxSparkCtr,clearFishingFlag,
        getSparkRecCtr,getThreadRecCtr,waitingFishingReplyFrom,
        incCtr,readSparkRec,readThreadRec,readTaskNotRec)
import qualified Control.Parallel.HdpH.Internal.Sparkpool as Sparkpool (run)
import Control.Parallel.HdpH.Internal.Threadpool
       (ThreadM, forkThreadM, stealThread, readMaxThreadCtrs)
import qualified Control.Parallel.HdpH.Internal.Threadpool as Threadpool
       (run, liftSparkM, liftCommM, liftIO)
import Control.Parallel.HdpH.Internal.Type.Par
import Control.Parallel.HdpH.Internal.IVar
import Data.IORef
import Control.Parallel.HdpH.Internal.Type.Msg

-----------------------------------------------------------------------------
-- RTS monad

-- The RTS monad hides monad stack (IO, CommM, SparkM, ThreadM) as abstract.
newtype RTS a = RTS { unRTS :: ThreadM RTS a }
                deriving (Functor, Monad)

-- Fork a new thread to execute the given 'RTS' action; the integer 'n'
-- dictates how much to rotate the thread pools (so as to avoid contention
-- due to concurrent access).
forkRTS :: Int -> RTS () -> RTS ThreadId
forkRTS n = liftThreadM . forkThreadM n . unRTS


-- Eliminate the whole RTS monad stack down the IO monad by running the given
-- RTS action 'main'; aspects of the RTS's behaviour are controlled by
-- the respective parameters in the given RTSConf.
-- NOTE: This function start various threads (for executing schedulers,
--       a message handler, and various timeouts). On normal termination,
--       all these threads are killed. However, there is no cleanup in the
--       event of aborting execution due to an exception. The functions
--       for doing so (see Control.Execption) all live in the IO monad.
--       Maybe they could be lifted to the RTS monad by using the monad-peel
--       package.
run_ :: RTSConf -> RTS () -> IO ()
run_ conf main = do
  let n = scheds conf
  unless (n > 0) $
    error "HdpH.Internal.Scheduler.run_: no schedulers"

  -- allocate n+1 empty thread pools (numbered from 0 to n)
  pools <- mapM (\ k -> do { pool <- Deque.emptyIO; return (k,pool) }) [0 .. n]

  -- fork nowork server (for clearing the "FISH outstanding" flag on NOWORK)
  noWorkServer <- newServer

  -- create semaphore for idle schedulers
  idleSem <- Sem.new

  -- fork wakeup server (periodically waking up racey sleeping scheds)
  wakeupServerTid <- forkIO $ Sem.signalPeriodically idleSem (wakeupDly conf)

  -- start the RTS
  Comm.run_ conf $
    Sparkpool.run conf noWorkServer idleSem $
      Threadpool.run pools $
        unRTS $
          rts n noWorkServer wakeupServerTid

    where
      -- RTS action
      rts :: Int -> ActionServer -> ThreadId -> RTS ()
      rts schds noWorkServer wakeupServerTid = do

        -- fork message handler (accessing thread pool 0)
        handlerTid <- forkRTS 0 handler

        -- fork schedulers (each accessing thread pool k, 1 <= k <= scheds)
        schedulerTids <- mapM (`forkRTS`  scheduler) [1 .. schds]

        -- run main RTS action
        main

        -- block waiting for shutdown barrier
        liftCommM Comm.waitShutdown

        -- print stats
        printFinalStats

        -- kill nowork server
        liftIO $ killServer noWorkServer

        -- kill wakeup server
        liftIO $ killThread wakeupServerTid

        -- kill message handler
        liftIO $ killThread handlerTid

        -- kill schedulers
        liftIO $ mapM_ killThread schedulerTids


-- lifting lower layers
liftThreadM :: ThreadM RTS a -> RTS a
liftThreadM = RTS

liftSparkM :: SparkM RTS a -> RTS a
liftSparkM = liftThreadM . Threadpool.liftSparkM

liftCommM :: CommM a -> RTS a
liftCommM = liftThreadM . Threadpool.liftCommM

liftIO :: IO a -> RTS a
liftIO = liftThreadM . Threadpool.liftIO


-- Return scheduler ID, that is ID of scheduler's own thread pool.
--schedulerID :: RTS Int
--schedulerID = liftThreadM poolID


-----------------------------------------------------------------------------
-- cooperative scheduling

-- Execute the given thread until it blocks or terminates.
execThread :: Thread RTS -> RTS ()
execThread (Atom m) =  m >>= maybe (return ()) execThread


-- Try to get a thread from a thread pool or the spark pool and execute it
-- until it blocks or terminates, whence repeat forever; if there is no
-- thread to execute then block the scheduler (ie. its underlying IO thread).
scheduler :: RTS ()
scheduler = getThread >>= scheduleThread


-- Execute given thread until it blocks or terminates, whence call 'scheduler'.
scheduleThread :: Thread RTS -> RTS ()
scheduleThread (Atom m) = m >>= maybe scheduler scheduleThread


-- Try to steal a thread from any thread pool (with own pool preferred);
-- if there is none, try to convert a spark from the spark pool;
-- if there is none too, block the scheduler such that the 'getThread'
-- action will be repeated on wake up.
-- NOTE: Sleeping schedulers should be woken up
--       * after new threads have been added to a thread pool,
--       * after new sparks have been added to the spark pool, and
--       * once the delay after a NOWORK message has expired.
getThread :: RTS (Thread RTS)
getThread = do
  maybe_thread <- liftThreadM stealThread
  case maybe_thread of
    Just thread -> return thread
    Nothing     -> do
      maybe_spark <- liftSparkM getSpark
      case maybe_spark of
       Just sparkClosure ->
        case sparkClosure of
         (Left supSparkClo) -> do
          return $ mkThread $ unClosure $ clo $ unClosure supSparkClo
         (Right spark) -> do
          return $ mkThread $ unClosure spark
       Nothing -> liftSparkM blockSched >> getThread



-- Converts 'Par' computations into threads.
mkThread :: ParM RTS a -> Thread RTS
mkThread p = unPar p $ \ _c -> Atom (return Nothing)

-----------------------------------------------------------------------------
-- pushed sparks


sendPUSH :: Task RTS -> NodeId -> RTS ()
sendPUSH spark target = do
  here <- liftCommM Comm.myNode
  if target == here
    then do
      -- short cut PUSH msg locally
      execSpark spark
    else do
      -- now send PUSH message to write IVar value to host
      let pushMsg = PUSH spark :: Msg RTS
      debug dbgMsgSend $
        show pushMsg ++ " ->> " ++ show target
      void $ liftCommM $ Comm.send target $ encodeLazy pushMsg

-- |Handle a PUSH message by converting the spark into a thread and
--  executing it immediately.
-- handlePUSH :: Msg RTS -> RTS ()
handlePUSH :: Msg RTS -> RTS ()
handlePUSH (PUSH spark) =
    -- Then convert the spark to a thread.
    -- This task is now going nowhere, it will
    -- either be evaluated here, or will be lost
    -- if this node dies
    execSpark spark

-- | Handles DEADNODE messages. Perform 3 procedures:
-- 1) If it is waiting for a fish reply from the deadNode,
--    then stop waiting, and begin fishing again.
-- 2) If spark in guard post was destined for failed node,
--    then empty the guard post. Do not put this spark back
--    into local sparkpool - it is the supervisors job to
--    reschedule the spark (which it does within its own sparkpool).
--    This is necessary to allow this node to once again
--    accept FISH messages ('waitingReqResponse' returns False again).
-- 3) Then, establish if the dead node was hosting any of
--    the supervised spark I created.
--    3a) identify all at-risk futures
--    3b) create replicas
--    3c) re-schedule non-evalauted tasks (empty associated IVars)
handleDEADNODE :: Msg RTS -> RTS ()
handleDEADNODE (DEADNODE deadNode) = do
    -- remove node from virtual machine
    liftCommM $ Comm.rmNode deadNode
    debug dbgFailure $
      "dead node detected: " ++ show deadNode

    -- 1) if waiting for FISH response from dead node, reset
    maybe_fishingReplyNode <- liftSparkM waitingFishingReplyFrom
    when (isJust maybe_fishingReplyNode) $ do
      let fishingReplyNode = fromJust maybe_fishingReplyNode
      when (fishingReplyNode == deadNode) $ do
        liftSparkM clearFishingFlag

    -- 2) If spark in guard post was destined for
    --    the failed node, put spark back in sparkpool
    liftSparkM $ popGuardPostToSparkpool deadNode

    -- 3a) identify all empty vulnerable futures
    emptyIVars <- liftIO $ vulnerableEmptyFutures deadNode :: RTS [(Int,IVar m a)]
    (emptyIVarsSparked,emptyIVarsPushed) <- partitionM wasSparked emptyIVars

    -- 3b) create replicas    
    replicatedSparks  <- mapM (liftIO . replicateSpark) emptyIVarsSparked
    replicatedThreads <- mapM (liftIO . replicateThread) emptyIVarsPushed

    -- 3c) schedule replicas
    mapM_ (liftSparkM . putSpark . Left . toClosure . fromJust) replicatedSparks
    mapM_ (execThread . mkThread . unClosure . fromJust) replicatedThreads

    -- for RTS stats
    replicateM_ (length emptyIVarsSparked) $ liftSparkM $ getSparkRecCtr  >>= incCtr
    replicateM_ (length emptyIVarsPushed)  $ liftSparkM $ getThreadRecCtr >>= incCtr

    -- for chaos monkey stats
    unless (null replicatedSparks) $ do
       debug dbgFailure $
         ("replicating sparks: " ++ show (length replicatedSparks))
       liftIO $ putStrLn ("replicating sparks: " ++ show (length replicatedSparks))
    unless (null replicatedThreads) $ do
       debug dbgFailure $
         ("replicating threads: " ++ show (length replicatedThreads))
       liftIO $ putStrLn ("replicating threads: " ++ show (length replicatedThreads))

  where
    wasSparked (_,v) = do
      e <- liftIO $ readIORef v
      let (Empty _ maybe_st) = e
          st = fromJust maybe_st
      return $ scheduling st == Sparked

handleHEARTBEAT :: Msg RTS -> RTS ()
handleHEARTBEAT (HEARTBEAT) = return ()

-- | Execute a spark (by converting it to a thread and executing).
execSpark :: Task RTS -> RTS ()
execSpark sparkClosure =
  case sparkClosure of
   (Left supSpClo) -> do
     let spark = unClosure supSpClo
     execThread $ mkThread $ unClosure (clo spark)
   (Right spark) -> do
    execThread $ mkThread $ unClosure spark

-----------------------------------------------------------------------------
-- message handler; only PUSH messages are actually handled here in this
-- module, other messages are relegated to module Sparkpool.

-- Message handler, running continously (in its own thread) receiving
-- and handling messages (some of which may unblock threads or create sparks)
-- as they arrive.
handler :: RTS ()
handler = do
  msg <- decodeLazy <$> liftCommM Comm.receive
  sparks <- liftSparkM readPoolSize
  debug dbgMsgRcvd $
    ">> " ++ show msg ++ " #sparks=" ++ show sparks
  case msg of
    -- eager placement
    PUSH{}       -> handlePUSH msg

    -- scheduling
    FISH{}       -> liftSparkM $ dispatch msg
    SCHEDULE{}   -> liftSparkM $ dispatch msg
    NOWORK{}     -> liftSparkM $ dispatch msg

    -- fault tolerance
    REQ{}        -> liftSparkM $ dispatch msg
    AUTH{}       -> liftSparkM $ dispatch msg
    DENIED{}     -> liftSparkM $ dispatch msg
    ACK{}        -> liftSparkM $ dispatch msg
    OBSOLETE{}   -> liftSparkM $ dispatch msg
    DEADNODE{}   -> handleDEADNODE msg
    HEARTBEAT{}  -> handleHEARTBEAT msg
  handler



-----------------------------------------------------------------------------
-- auxiliary stuff

-- Print stats (#sparks, threads, FISH, ...) at appropriate debug level.
printFinalStats :: RTS ()
printFinalStats = do
  fishes <- liftSparkM $ readFishSentCtr
  schds <- liftSparkM $ readSparkRcvdCtr
  sparks <- liftSparkM $ readSparkGenCtr
  recoveredSparks <- liftSparkM $ readSparkRec
  recoveredThreads <- liftSparkM $ readThreadRec
  lostCompletedTasks <- liftSparkM $ readTaskNotRec
  max_sparks  <- liftSparkM $ readMaxSparkCtr
  maxs_threads <- liftThreadM $ readMaxThreadCtrs
  debug dbgStats $ "#SPARK=" ++ show sparks ++ "   " ++
                   "max_SPARK=" ++ show max_sparks ++ "   " ++
                   "max_THREAD=" ++ show maxs_threads
  debug dbgStats $ "#FISH_sent=" ++ show fishes ++ "   " ++
                   "#SCHED_rcvd=" ++ show schds
  debug dbgStats $ "#Recovered_SPARK=" ++ show recoveredSparks ++ "   " ++
                   "#Recovered_THREAD=" ++ show recoveredThreads ++ "   " ++
                   "#NotRecovered_TASK=" ++ show lostCompletedTasks


debug :: Int -> String -> RTS ()
debug level message = liftIO $ Location.debug level message


-- | Used to split those remote tasks that have and have not
--   been completely evaluated i.e. the associated IVar is empty.
partitionM :: Monad m => (a -> m Bool) -> [a] -> m ([a],[a])
partitionM p xs = do
    (f,g) <- pMHelper p xs
    return (f [], g [])
  where
    pMHelper _ = foldM help (id,id)
    help (f,g) x = do
      b <- p x
      return (if b then (f . (x:),g) else (f,g . (x:)))
