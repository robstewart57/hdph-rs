-- Spark pool and fishing
--
-- Visibility: HdpH, HdpH.Internal
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 02 Jul 2011
--
-----------------------------------------------------------------------------

{-# LANGUAGE ScopedTypeVariables #-}  -- req'd for type annotations


module Control.Parallel.HdpH.Internal.Sparkpool
  ( -- * spark pool monad
    SparkM,      -- synonym: SparkM m = ReaderT <State m> CommM
    run,         -- :: RTSConf -> ActionServer -> Sem -> SparkM m a -> CommM a
    liftCommM,   -- :: Comm a -> SparkM m a
    liftIO,      -- :: IO a -> SparkM m a

    -- * blocking and unblocking idle schedulers
    blockSched,  -- :: SparkM m ()
    wakeupSched, -- :: Int -> SparkM m ()

    -- * local (ie. scheduler) access to spark pool
    getFishingFlag,
    clearFishingFlag,
    waitingFishingReplyFrom,
    getSpark,    -- :: Int -> SparkM m (Maybe (Spark m))
    putSpark,    -- :: Int -> Spark m -> SparkM m ()

    -- * Allow scheduler to pop guard post to sparkpool on 'DEADNODE' message
    popGuardPostToSparkpool,

    -- * messages
    Msg(..),         -- instances: Show, NFData, Serialize

    -- * handle messages related to fishing
    dispatch,        -- :: Msg m a -> SparkM m ()
    handleFISH,      -- :: Msg m a -> SparkM m ()
    handleSCHEDULE,  -- :: Msg m a -> SparkM m ()
    handleNOWORK,    -- :: Msg m a -> SparkM m ()

    -- * access to stats data
    readPoolSize,      -- :: SparkM m Int
    readFishSentCtr,   -- :: SparkM m Int
    readSparkRcvdCtr,  -- :: SparkM m Int
    readMaxSparkCtr,   -- :: SparkM m Int
    readSparkGenCtr,   -- :: SparkM m Int
    readSparkConvCtr,   -- :: SparkM m Int

    -- * access to fault recovery stats data
    incCtr,
    getSparkRecCtr,
    getThreadRecCtr,
    getTaskNotRecCtr,
    readSparkRec,
    readThreadRec,
    readTaskNotRec,
  ) where

import Prelude hiding (error)
import Control.Concurrent (threadDelay)
import Control.Monad (unless, when, replicateM_, void)
import Control.Monad.Reader (ReaderT, runReaderT, ask)
import Control.Monad.Trans (lift)
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicModifyIORef)
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as Set (size, singleton, notMember)
import System.Random (randomRIO)
import Control.Parallel.HdpH.Conf
       (RTSConf(maxHops,maxFish, minSched, minFishDly, maxFishDly))
import Control.Parallel.HdpH.Internal.Comm (CommM)
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (liftIO, send, nodes, myNode, allNodes)
import Control.Parallel.HdpH.Internal.Data.Deque
       (DequeIO, emptyIO, pushBackIO, popFrontIO, popBackIO,
        lengthIO, maxLengthIO)
import Control.Parallel.HdpH.Internal.Data.Sem (Sem)
import qualified Control.Parallel.HdpH.Internal.Data.Sem as Sem (wait, signal)
import Control.Parallel.HdpH.Internal.Location (NodeId, dbgMsgSend, dbgSpark, dbgFailure, error)
import qualified Control.Parallel.HdpH.Internal.Location as Location (debug)
import Control.Parallel.HdpH.Internal.Misc (encodeLazy, ActionServer, reqAction)
import Control.Parallel.HdpH.Internal.Type.Msg
import Control.Parallel.HdpH.Internal.Type.Par
import Control.Parallel.HdpH.Internal.IVar
import Control.Parallel.HdpH.Closure (Closure,unClosure)
import Control.Parallel.HdpH.Internal.Type.GRef (atT)


-----------------------------------------------------------------------------
-- SparkM monad

-- 'SparkM m' is a reader monad sitting on top of the 'CommM' monad;
-- the parameter 'm' abstracts a monad (cf. module Control.Parallel.Control.Parallel.HdpH.Internal.Type.Par).
type SparkM m = ReaderT (State m) CommM

type GuardPost m = Maybe (OccupiedGuardPost m)
data OccupiedGuardPost m = OccupiedGuardPost
  {
    guardedSpark :: Closure (SupervisedSpark m)
  , destinedFor  :: NodeId -- not strictly needed; sanity check.
  }


-- spark pool state (mutable bits held in IORefs and the like)
data State m =
  State {
    s_conf       :: RTSConf,               -- config data
    s_pool       :: DequeIO (Task m),  -- actual spark pool
    s_guard_post
                 :: IORef (GuardPost m), -- spark waiting for scheduling authorisation
    s_sparkOrig  :: IORef (Maybe NodeId),  -- origin of most recent spark recvd
    s_fishing    :: IORef (Maybe NodeId),            -- True iff FISH outstanding
    s_noWork     :: ActionServer,          -- for clearing "FISH outstndg" flag
    s_idleScheds :: Sem,                   -- semaphore for idle schedulers
    s_fishSent   :: IORef Int,             --  m -> SparkM m ()
    s_sparkRcvd  :: IORef Int,             -- #sparks received
    s_sparkGen   :: IORef Int,             -- #sparks generated
    s_sparkConv  :: IORef Int,             -- #sparks converted
    s_sparkRec   :: IORef Int,
    s_threadRec  :: IORef Int,
    s_taskNotRec :: IORef Int }

-- Eliminates the 'SparkM' layer by executing the given 'SparkM' action on
-- an empty spark pool; expects a config data, an action server (for
-- clearing "FISH outstanding" flag) and a semaphore (for idle schedulers).
run :: RTSConf -> ActionServer -> Sem -> SparkM m a -> CommM a
run conf noWorkServer idleSem action = do
  -- set up spark pool state
  pool      <- Comm.liftIO emptyIO
  guardPost <- Comm.liftIO $ newIORef Nothing
  sparkOrig <- Comm.liftIO $ newIORef Nothing
  fishing   <- Comm.liftIO $ newIORef Nothing
  fishSent  <- Comm.liftIO $ newIORef 0
  sparkRcvd <- Comm.liftIO $ newIORef 0
  sparkGen  <- Comm.liftIO $ newIORef 0
  sparkConv <- Comm.liftIO $ newIORef 0
  sparkRec  <- Comm.liftIO $ newIORef 0
  threadRec <- Comm.liftIO $ newIORef 0
  taskNotRec <- Comm.liftIO $ newIORef 0
  let s0 = State { s_conf       = conf,
                   s_pool       = pool,
                   s_guard_post = guardPost,
                   s_sparkOrig  = sparkOrig,
                   s_fishing    = fishing,
                   s_noWork     = noWorkServer,
                   s_idleScheds = idleSem,
                   s_fishSent   = fishSent,
                   s_sparkRcvd  = sparkRcvd,
                   s_sparkGen   = sparkGen,
                   s_sparkConv  = sparkConv,
                   s_sparkRec   = sparkRec,
                   s_threadRec  = threadRec,
                   s_taskNotRec = taskNotRec }
  -- run monad
  runReaderT action s0


-- Lifting lower layers.
liftCommM :: CommM a -> SparkM m a
liftCommM = lift

liftIO :: IO a -> SparkM m a
liftIO = liftCommM . Comm.liftIO


-----------------------------------------------------------------------------
-- access to state

-- | local spark pool
getPool :: SparkM m (DequeIO (Task m))
getPool = s_pool <$> ask

getGuardPost :: SparkM m (IORef (GuardPost m))
getGuardPost = s_guard_post <$> ask

-- | adds a spark to the guard post
guardSpark :: Closure (SupervisedSpark m) -> NodeId -> SparkM m ()
guardSpark newSupervisedSpark fisher = do
    x <- getGuardPost
    liftIO $ atomicModifyIORef x $ \currentGuardPost ->
        if isJust currentGuardPost
         then error "Another spark already occupying guard post"
         else
           let newGuardPost = OccupiedGuardPost
                 { guardedSpark = newSupervisedSpark
                 , destinedFor = fisher }
           in (Just newGuardPost,())

-- | pops guard post back to sparkpool iff
--   a) the guard post is occupied &&
--   b) the the guarded spark is destined for dead node
--   This is called when DEADNODE message is received
--   by any node.
popGuardPostToSparkpool :: NodeId -> SparkM m ()
popGuardPostToSparkpool deadNode = do
    x <- getGuardPost
    poppedSpark <- liftIO $ atomicModifyIORef x $ \currentGuardPost ->
        if isNothing currentGuardPost
          then (Nothing,Nothing)

          else
            let destination = destinedFor $ fromJust currentGuardPost
            in if destination == deadNode
              then (Nothing,currentGuardPost) -- move from guard post to sparkpool
              else (currentGuardPost,Nothing) -- leave guardpost (and sparkpool) alone
    -- put the guarded spark back in the sparkpool
    when (isJust poppedSpark) $ do
      pool <- getPool
      liftIO $ pushBackIO pool (Left $ guardedSpark (fromJust poppedSpark))


-- | pops the spark in the guard post
toppleGuardPost :: SparkM m (Maybe (Closure (SupervisedSpark m)))
toppleGuardPost = do
    x <- getGuardPost
    liftIO $ atomicModifyIORef x $ \currentGuardPost ->
        if isNothing currentGuardPost
          then (Nothing,Nothing) -- error "no spark to topple"
          else (Nothing,Just $ guardedSpark $ fromJust currentGuardPost)

-- | returns True iff there is a spark in the guard post
waitingReqResponse :: SparkM m Bool
waitingReqResponse = do
    maybe_spark <- getGuardPost >>= liftIO . readIORef
    return $ isJust maybe_spark

readPoolSize :: SparkM m Int
readPoolSize = getPool >>= liftIO . lengthIO

getSparkOrigHist :: SparkM m (IORef (Maybe NodeId))
getSparkOrigHist = s_sparkOrig <$> ask

readSparkOrigHist :: SparkM m (Maybe NodeId)
readSparkOrigHist = getSparkOrigHist >>= liftIO . readIORef

clearSparkOrigHist :: SparkM m ()
clearSparkOrigHist = do
  sparkOrigHistRef <- getSparkOrigHist
  liftIO $ writeIORef sparkOrigHistRef Nothing

updateSparkOrigHist :: NodeId -> SparkM m ()
updateSparkOrigHist mostRecentOrigin = do
  sparkOrigHistRef <- getSparkOrigHist
  liftIO $ writeIORef sparkOrigHistRef (Just mostRecentOrigin)

getFishingFlagIORef :: SparkM m (IORef (Maybe NodeId))
getFishingFlagIORef = s_fishing <$> ask

getFishingFlag :: SparkM m Bool
getFishingFlag = do
  ioref <- s_fishing <$> ask
  maybe_victim <- liftIO $ readIORef ioref
  return $ isJust maybe_victim

setFishingFlag :: NodeId -> SparkM m ()
setFishingFlag victim = do
  s_fishing <$> ask >>= \ref -> liftIO (atomicWriteIORef ref (Just victim))

waitingFishingReplyFrom :: SparkM m (Maybe NodeId)
waitingFishingReplyFrom = do
  ioref <- s_fishing <$> ask
  liftIO $ readIORef ioref

clearFishingFlag = do
  s_fishing <$> ask >>= \ref -> liftIO (atomicWriteIORef ref Nothing)

getNoWorkServer :: SparkM m ActionServer
getNoWorkServer = s_noWork <$> ask

getIdleSchedsSem :: SparkM m Sem
getIdleSchedsSem = s_idleScheds <$> ask

getFishSentCtr :: SparkM m (IORef Int)
getFishSentCtr = s_fishSent <$> ask

readFishSentCtr :: SparkM m Int
readFishSentCtr = getFishSentCtr >>= readCtr

getSparkRcvdCtr :: SparkM m (IORef Int)
getSparkRcvdCtr = s_sparkRcvd <$> ask

readSparkRcvdCtr :: SparkM m Int
readSparkRcvdCtr = getSparkRcvdCtr >>= readCtr

getSparkGenCtr :: SparkM m (IORef Int)
getSparkGenCtr = s_sparkGen <$> ask

readSparkGenCtr :: SparkM m Int
readSparkGenCtr = getSparkGenCtr >>= readCtr

getSparkConvCtr :: SparkM m (IORef Int)
getSparkConvCtr = s_sparkConv <$> ask

readSparkConvCtr :: SparkM m Int
readSparkConvCtr = getSparkConvCtr >>= readCtr

readMaxSparkCtr :: SparkM m Int
readMaxSparkCtr = getPool >>= liftIO . maxLengthIO

getMaxHops :: SparkM m Int
getMaxHops = maxHops <$> s_conf <$> ask

getMaxFish :: SparkM m Int
getMaxFish = maxFish <$> s_conf <$> ask

getMinSched :: SparkM m Int
getMinSched = minSched <$> s_conf <$> ask

getMinFishDly :: SparkM m Int
getMinFishDly = minFishDly <$> s_conf <$> ask

getMaxFishDly :: SparkM m Int
getMaxFishDly = maxFishDly <$> s_conf <$> ask

getSparkRecCtr :: SparkM m (IORef Int)
getSparkRecCtr = s_sparkRec <$> ask

getThreadRecCtr :: SparkM m (IORef Int)
getThreadRecCtr = s_threadRec <$> ask

getTaskNotRecCtr :: SparkM m (IORef Int)
getTaskNotRecCtr = s_taskNotRec <$> ask

readSparkRec :: SparkM m Int
readSparkRec = getSparkRecCtr >>= readCtr

readThreadRec :: SparkM m Int
readThreadRec = getThreadRecCtr >>= readCtr

readTaskNotRec :: SparkM m Int
readTaskNotRec = getTaskNotRecCtr >>= readCtr

-----------------------------------------------------------------------------
-- blocking and unblocking idle schedulers

-- Put executing scheduler to
blockSched :: SparkM m ()
blockSched = getIdleSchedsSem >>= liftIO . Sem.wait


-- Wake up 'n' sleeping schedulers.
wakeupSched :: Int -> SparkM m ()
wakeupSched n = getIdleSchedsSem >>= liftIO . replicateM_ n . Sem.signal


-----------------------------------------------------------------------------
-- local access to spark pool

-- Get a spark from the front of the spark pool, if there is any;
-- possibly send a FISH message and update stats (ie. count sparks converted);
getSpark :: SparkM m (Maybe (Task m))
getSpark = do
  pool <- getPool
--  maybe_spark <- liftIO $ popFrontIO pool
  maybe_spark <- liftIO $ popBackIO pool
  sendFISH
  case maybe_spark of
    Just _  -> do getSparkConvCtr >>= incCtr
                  sparks <- liftIO $ lengthIO pool
                  debug dbgSpark $
                    "#sparks=" ++ show sparks ++ " (spark converted)"
                  return maybe_spark
    Nothing -> return maybe_spark


-- Put a new spark at the back of the spark pool, wake up 1 sleeping scheduler,
-- and update stats (ie. count sparks generated locally);
putSpark :: Task m -> SparkM m ()
putSpark spark = do
  pool <- getPool
  liftIO $ pushBackIO pool spark
  wakeupSched 1
  getSparkGenCtr >>= incCtr
  sparks <- liftIO $ lengthIO pool
  debug dbgSpark $
    "#sparks=" ++ show sparks ++ " (spark created)"

-----------------------------------------------------------------------------
-- fishing and the like

-- Send a FISH message, but only if there is no FISH outstanding and the
-- number of sparks in the pool is less or equal to the 'maxFish' parameter;
-- the target is the sender of the most recent SCHEDULE message, if a
-- SCHEDULE message has yet been received, otherwise the target is random.
sendFISH :: SparkM m ()
sendFISH = do
  pool <- getPool
  isFishing <- getFishingFlag
  unless isFishing $ do
    -- no FISH currently outstanding
    nodes <- liftCommM Comm.nodes
    maxFish' <- getMaxFish
    sparks <- liftIO $ lengthIO pool
    when (nodes > 1 && sparks <= maxFish') $ do
      -- there are other nodes and the pool has too few sparks;
      -- set flag indicating that we are going to send a FISH

      -- ok <- setFlag fishingFlag
      --when True $ do

      -- flag was clear before: go ahead sending FISH;
      -- construct message
      fisher <- liftCommM Comm.myNode
      -- hops <- getMaxHops
      -- target is node where most recent spark came from (if such exists)

      maybe_target <- readSparkOrigHist
      target <- case maybe_target of
                  Just node -> return node
                  Nothing   -> do allNodes <- liftCommM Comm.allNodes
                                  let avoidNodes = Set.singleton fisher
                                  -- select random target (other than fisher)
                                  randomOtherElem avoidNodes allNodes nodes

      setFishingFlag target
      let fishMsg = FISH fisher :: Msg m
      -- send FISH message
      debug dbgMsgSend $
        show fishMsg ++ " ->> " ++ show target
      void $ liftCommM $ Comm.send target $ encodeLazy fishMsg
      getFishSentCtr >>= incCtr

-- Dispatch FISH, SCHEDULE and NOWORK messages to their respective handlers.
dispatch :: Msg m -> SparkM m ()
dispatch msg@FISH{}     = handleFISH msg
dispatch msg@SCHEDULE{} = handleSCHEDULE msg
dispatch msg@NOWORK     = handleNOWORK msg
dispatch msg@REQ{}      = handleREQ msg
dispatch msg@AUTH{}     = handleAUTH msg
dispatch msg@DENIED{}   = handleDENIED msg
dispatch msg@ACK{}      = handleACK msg
dispatch msg@OBSOLETE{} = handleOBSOLETE msg
dispatch msg = error $ "Control.Parallel.Control.Parallel.HdpH.Internal.Sparkpool.dispatch: " ++ show msg ++ " unexpected"

handleFISH :: Msg m -> SparkM m ()
handleFISH (FISH fisher) = do
  here <- liftCommM Comm.myNode
  sparks <- readPoolSize
  minSchd <- getMinSched
  waitingAuthorisation <- waitingReqResponse

  -- send SCHEDULE if pool has enough sparks
  -- and the node is not waiting for a response
  -- from a REQ message (i.e. NOWORK or SCHEDULE)
  done <- if sparks < minSchd || waitingAuthorisation
             then return False
             else do
--               maybe_spark <- getPool >>= liftIO . popBackIO
               maybe_spark <- getPool >>= liftIO . popFrontIO
               case maybe_spark of
                 Just spark ->
                  case spark of

                   (Left supSparkClo) -> do

                      -- 1) Move supervised spark to guard post.
                      --    This also has the effect of blocking
                      --    whilst waiting from the supervisor
                      --    on the REQ response below
                      guardSpark supSparkClo fisher

                      -- 2) Send REQ message to supervisor of spark
                      let supSpark = unClosure supSparkClo
                      let ref = remoteRef supSpark
                          seqN = thisReplica supSpark
                          supervisor = atT ref
                          requestMsg = REQ ref seqN here fisher
                      debug dbgMsgSend $
                        show requestMsg ++ " ->> " ++ show supervisor
                      -- shortcut message delivery
                      if (supervisor == here)
                       then handleREQ requestMsg
                       else void $ liftCommM $ Comm.send supervisor $ encodeLazy requestMsg
                      return True

                   (Right _normalSparkClo) -> do
                      -- fault oblivious scheduling
                      let schedMsg = SCHEDULE spark here
                      debug dbgMsgSend $
                        show schedMsg ++ " ->> " ++ show fisher
                      void $ liftCommM $ Comm.send fisher $ encodeLazy schedMsg
                      return True

                 Nothing -> return False

  unless done $ do
    let noWorkMsg = NOWORK :: Msg m
    debug dbgMsgSend $
      show noWorkMsg ++ " ->> " ++ show fisher
    void (liftCommM $ Comm.send fisher $ encodeLazy noWorkMsg)


{- No, we cannot use hops in FT scheduler.
-- Reason: If DEADNODE is received, and a node is waiting
-- for a FISH reply, it may be waiting for a reply from the dead
-- message. A node should start fishing again. If the FISH has been
-- forwarded to a node that then fails, a node would be waiting forever
-- for a reply.

    -- no SCHEDULE sent; check whether to forward FISH
    nodes <- liftCommM $ Comm.nodes
    let avoidNodes = Set.fromList [fisher, target, here]
    if hops > 0 && nodes > Set.size avoidNodes
       then do  -- fwd FISH to random node (other than those in avoidNodes)
         allNodes <- liftCommM $ Comm.allNodes
         node <- randomOtherElem avoidNodes allNodes nodes
         let fishMsg = FISH fisher target (hops - 1) :: Msg m
         debug dbgMsgSend $
           show fishMsg ++ " ->> " ++ show node
         attempt <- liftCommM $ Comm.send node $ encodeLazy fishMsg
         case attempt of
            (Left (NT.TransportError _ e)) -> do -- either SendClosed or SendFailed
                debug dbgFailure $
                    "FISH delivery unsuccessful to "
                    ++ show target
                    ++ "\t" ++ show e
                -- tell fisher that the FISH send failed
                let fishFailedMsg = FISHFAILED
                -- send FISHFAILED message to the original fisher
                debug dbgMsgSend $
                   show fishFailedMsg ++ " ->> " ++ show fisher
                void $ liftCommM $ Comm.send fisher $ encodeLazy fishFailedMsg
            (Right ()) -> return ()

       else do  -- notify fisher that there is no work
         -}



-- Handle a SCHEDULE message;
-- * puts the spark at the front of the spark pool,
-- * records spark sender and updates stats, and
-- * clears the "FISH outstanding" flag.
handleSCHEDULE :: Msg m -> SparkM m ()
handleSCHEDULE (SCHEDULE spark sender) = do
    -- 1) If a supervised spark, send ACK to supervisor
    when (taskSupervised spark) $ ackSupervisedTask

    -- put spark into pool
    pool <- getPool
--    liftIO $ pushFrontIO pool spark
    liftIO $ pushBackIO pool spark
    -- record sender of spark
    updateSparkOrigHist sender
    -- update stats
    getSparkRcvdCtr >>= incCtr
    -- clear FISHING flag
    --void $ getFishingFlag >>= clearFlag
    clearFishingFlag
    return ()
  where
   ackSupervisedTask :: SparkM m ()
   ackSupervisedTask = do
    me <- liftCommM Comm.myNode
    let (Left supSpkClo) = spark
        supSpark = unClosure supSpkClo
        ref = remoteRef supSpark
        seqN = thisReplica supSpark
    let ackMsg = ACK ref seqN me
        supervisor = atT ref
    debug dbgMsgSend $
       show ackMsg ++ " ->> " ++ show supervisor
    void (liftCommM $ Comm.send supervisor $ encodeLazy ackMsg)

-- Handle a NOWORK message;
-- asynchronously, after a random delay, clear the "FISH outstanding" flag
-- and wake one scheduler (if some are sleeping) to resume fishing.
-- Rationale for random delay: to prevent FISH flooding when there is
--   (almost) no work.
handleNOWORK :: Msg m -> SparkM m ()
handleNOWORK NOWORK = do
  clearSparkOrigHist
  fishingFlagRef   <- getFishingFlagIORef
  noWorkServer  <- getNoWorkServer
  idleSchedsSem <- getIdleSchedsSem
  minDelay      <- getMinFishDly
  maxDelay      <- getMaxFishDly
  -- compose delay and clear flag action
  let action = do -- random delay
                  delay <- randomRIO (minDelay, max minDelay maxDelay)
                  threadDelay delay
                  -- clear fishing flag
                  atomicModifyIORef fishingFlagRef $ const (Nothing, ())

                  -- wakeup 1 sleeping scheduler (to fish again)
                  Sem.signal idleSchedsSem
  -- post action request to server
  liftIO $ reqAction noWorkServer action

------------- Fault tolerance handling

-- REQ is sent to the supervisor of a spark
handleREQ :: Msg m -> SparkM m ()
handleREQ (REQ taskRef seqN from to) = do
    -- check that task has the highest sequence number
    maybe_isNewest <- liftIO $ isNewestReplica taskRef seqN
    if isNothing maybe_isNewest
      then do -- another copy of the task has completed
        let obsoleteMsg = (OBSOLETE to)
        debug dbgMsgSend $
          show obsoleteMsg ++ " ->> " ++ show from
        void $ liftCommM $ Comm.send from $ encodeLazy obsoleteMsg
      else do
       if not (fromJust maybe_isNewest) -- not the newest copy
        then do
         let obsoleteMsg = (OBSOLETE to)
         debug dbgMsgSend $
           show obsoleteMsg ++ " ->> " ++ show from
         void $ liftCommM $ Comm.send from $ encodeLazy obsoleteMsg
        else do
         nodes <- liftCommM Comm.allNodes
         -- check fisher hasn't died in the meantime (from verified model)
         if (to `elem` nodes)
          then do
           loc <- liftIO $ fromJust <$> locationOfTask taskRef
           case loc of
             OnNode current -> do
               if current == from
                then do -- authorise the schedule
                  -- update the book keeping to InTransition
                  liftIO $ taskInTransition taskRef from to
                  -- Send AUTH to owner (from)
                  let authMsg = AUTH taskRef to
                  debug dbgMsgSend $
                    show authMsg ++ " ->> " ++ show from
                  here <- liftCommM $ Comm.myNode
                  -- shortcutting message delivery
                  if (from == here)
                   then handleAUTH authMsg
                   else void $ liftCommM $ Comm.send from $ encodeLazy authMsg
                else error "spark not on the peer we expected"
             InTransition _ _ -> do
               let deniedMsg = DENIED to
               debug dbgMsgSend $
                 show deniedMsg ++ " ->> " ++ show from
               void $ liftCommM $ Comm.send from $ encodeLazy deniedMsg
          -- fisher has died in the meantime, and removed from VM (allNodes)
          else do
           let deniedMsg = DENIED to
           debug dbgMsgSend $
              show deniedMsg ++ " ->> " ++ show from
           void $ liftCommM $ Comm.send from $ encodeLazy deniedMsg

-- 1) Send a SCHEDULE to the fisher
-- 2) Empty the guard post
handleAUTH :: Msg m -> SparkM m ()
handleAUTH (AUTH taskRef fisher) = do
    -- pops the guarded spark from the guard post
    maybe_spark <- toppleGuardPost
    if (isJust maybe_spark)
     then do
      let spark = fromJust maybe_spark
      let ref = remoteRef $ unClosure spark
      when (taskRef /= ref) $
         error $ "Guarded task is not the one authorised with AUTH\n"
           ++ show taskRef ++ " /= " ++ show ref

      me <- liftCommM Comm.myNode
      let scheduleMsg = SCHEDULE (Left spark) me
      debug dbgMsgSend $
        show scheduleMsg ++ " ->> " ++ show fisher
      void $ liftCommM $ Comm.send fisher $ encodeLazy scheduleMsg
     else debug dbgFailure $  -- guard post empty
          "AUTH message arrived, but guard post empty. " ++
          "DEADNODE message must be have been received about thief, flushing guard post."

-- 1) Send NOWORK message to the fisher
-- 2) Remove spark from guard post,
--    push back into sparkpool.
-- 3) reset isFishing spark
handleDENIED :: Msg m -> SparkM m ()
handleDENIED (DENIED fisher) = do
    -- 1)
    let noworkMsg = NOWORK
    debug dbgMsgSend $
      show noworkMsg ++ " ->> " ++ show fisher
    void $ liftCommM $ Comm.send fisher $ encodeLazy noworkMsg

    -- 2)
    maybe_spark <- toppleGuardPost
    when (isJust maybe_spark) $ putSpark (Left (fromJust maybe_spark))

    -- 3)
    --void $ getFishingFlag >>= clearFlag
    clearFishingFlag

handleACK :: Msg m -> SparkM m ()
handleACK (ACK taskRef seqN newNode) = do
  maybe_isNewest <- liftIO $ isNewestReplica taskRef seqN
  if isNothing maybe_isNewest
    then -- not supervised
      liftIO $ taskOnNode taskRef newNode
    else do
      if fromJust maybe_isNewest -- valid copy, modify book keeping
        then liftIO $ taskOnNode taskRef newNode
        else debug dbgFailure $  -- obsolete copy
          "received ACK for old task copy, ignoring."

-- 1) topple obsolete spark from guard post (do NOT put back in sparkpool)
-- 2) return NOWORK to the fisher
handleOBSOLETE :: Msg m -> SparkM m ()
handleOBSOLETE (OBSOLETE fisher) = do
    -- 1)
    void toppleGuardPost

    -- 2)
    let noworkMsg = NOWORK
    debug dbgMsgSend $
      show noworkMsg ++ " ->> " ++ show fisher
    void $ liftCommM $ Comm.send fisher $ encodeLazy noworkMsg


-----------------------------------------------------------------------------
-- auxiliary stuff

{- Not used

readFlag :: IORef Bool -> SparkM m Bool
readFlag = liftIO . readIORef

-- Sets given 'flag'; returns True iff 'flag' did actually change.
setFlag :: IORef Bool -> SparkM m Bool
setFlag flag = liftIO $ atomicModifyIORef flag $ \ v -> (True, not v)

-- Clears given 'flag'; returns True iff 'flag' did actually change.
clearFlag :: IORef Bool -> SparkM m Bool
clearFlag flag = liftIO $ atomicModifyIORef flag $ \ v -> (False, v)
-}

readCtr :: IORef Int -> SparkM m Int
readCtr = liftIO . readIORef

incCtr :: IORef Int -> SparkM m ()
incCtr ctr = liftIO $ atomicModifyIORef ctr $ \ v ->
                        let v' = v + 1 in v' `seq` (v', ())


-- 'randomOtherElem avoid xs n' returns a random element of the list 'xs'
-- different from any of the elements in the set 'avoid'.
-- Requirements: 'n <= length xs' and 'xs' contains no duplicates.
randomOtherElem :: Ord a => Set a -> [a] -> Int -> SparkM m a
randomOtherElem avoid xs n = do
  let candidates = filter (`Set.notMember` avoid) xs
  -- length candidates == length xs - Set.size avoid >= n - Set.size avoid
  i <- liftIO $ randomRIO (0, n - Set.size avoid - 1)
  -- 0 <= i <= n - Set.size avoid - 1 < length candidates
  return (candidates !! i)


-- debugging
debug :: Int -> String -> SparkM m ()
debug level message = liftIO $ Location.debug level message


----------
-- part of base library in GHC 7.6

-- | Variant of 'writeIORef' with the \"barrier to reordering\" property that
-- 'atomicModifyIORef' has.
atomicWriteIORef :: IORef a -> a -> IO ()
atomicWriteIORef ref a = do
    x <- atomicModifyIORef ref (const (a, ()))
    x `seq` return ()
