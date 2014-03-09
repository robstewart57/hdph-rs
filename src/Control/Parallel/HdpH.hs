

-- HdpH programming interface
--
-- Author: Patrick Maier, Rob Stewart
-----------------------------------------------------------------------------

{-# LANGUAGE GeneralizedNewtypeDeriving #-}  -- for 'GIVar'
{-# LANGUAGE TypeSynonymInstances #-}        -- for 'Par'
{-# LANGUAGE FlexibleInstances #-}           -- for 'Par'
{-# LANGUAGE TemplateHaskell #-}             -- for 'mkClosure', etc.
{-# LANGUAGE RankNTypes #-}             -- for 'mkClosure', etc.
{-# LANGUAGE NoMonomorphismRestriction #-}

module Control.Parallel.HdpH
  ( -- * runtime system
    RTS,           -- * -> *; instances: Functor, Monad
    runRTS_,       -- :: RTSConf -> RTS () -> IO ()
    isMainRTS,     -- :: RTS Bool
    shutdownRTS,   -- :: RTS ()
    liftIO,        -- :: IO a -> RTS a

    -- * runtime system configuration
    module Control.Parallel.HdpH.Conf,

    -- * Par monad
    Par,        -- * -> *; instances: Functor, Monad
    runPar,     -- :: Par a -> RTS a
    runParIO_,  -- :: RTSConf -> Par () -> IO ()
    runParIO,   -- :: RTSConf -> Par a -> IO (Maybe a)
    parseOpts,

    -- * operations in the Par monad
    done,       -- :: Par a
    myNode,     -- :: Par NodeId
    allNodes,   -- :: Par [NodeId]
    io,         -- :: IO a -> Par a
    eval,       -- :: a -> Par a
    force,      -- :: (NFData a) => a -> Par a
    fork,       -- :: Par () -> Par ()
    new,        -- :: Par (IVar a)
    put,        -- :: IVar a -> a -> Par ()
    get,        -- :: IVar a -> Par a
    poll,       -- :: IVar a -> Par (Maybe a)
    probe,      -- :: IVar a -> Par Bool
    glob,       -- :: IVar (Closure a) -> Par (GIVar (Closure a))
    rput,       -- :: GIVar (Closure a) -> Closure a -> Par ()

    -- * spawn family
    spawn,      -- :: Closure (Par (Closure a)) -> Par (IVar (Closure a))
    spawnAt,    -- :: Closure (Par (Closure a)) -> NodeId -> Par (IVar (Closure a))
    supervisedSpawn,   -- :: Closure (Par (Closure a)) -> Par (GIVar (Closure a))
    supervisedSpawnAt, -- :: Closure (Par (Closure a)) -> NodeId -> Par (IVar (Closure a))

    -- * Internal HdpH-RS primitives (exposed for Strategies)
    spark,      -- :: Closure (Par ()) -> Par ()
    pushTo,     -- :: Closure (Par ()) -> NodeId -> Par ()

    -- * locations
    NodeId,  -- instances: Eq, Ord, Show, NFData, Binary

    -- * local and global IVars
    IVar,    -- * -> *; instances: none
    GIVar,   -- * -> *; instances: Eq, Ord, Show, NFData, Binary
    at,      -- :: GIVar a -> NodeId

    -- * explicit Closures
    module Control.Parallel.HdpH.Closure,

    -- * Static decloaration for internal Closures in HdpH and Control.Parallel.HdpH.Closure
    declareStatic, -- :: StaticDeclo

    -- * chaos monkey unit test
    chaosMonkeyUnitTest -- :: (Eq a, Show a) => RTSConf -> String -> a -> Par a -> IO ()
  ) where

import Prelude hiding (error)
import Control.DeepSeq (NFData, deepseq)
import Control.Exception (evaluate)
import Control.Monad (when,void)
import Data.List
import Data.List.Split
import Data.Functor ((<$>))
import Data.Monoid (mconcat)
import Data.Binary (Binary)
import Test.HUnit.Base
import Test.HUnit.Text

import Control.Parallel.HdpH.Conf                            -- re-export whole module
import Control.Parallel.HdpH.Closure hiding (declareStatic)  -- re-export almost whole module
import qualified Control.Parallel.HdpH.Closure (declareStatic)
import qualified Control.Parallel.HdpH.Internal.Comm as Comm
       (myNode, allNodes, isMain, shutdown)
import qualified Control.Parallel.HdpH.Internal.IVar as IVar (IVar, GIVar)
import Control.Parallel.HdpH.Internal.IVar
       (newIVar, newSupervisedIVar, putIVar, getIVar, pollIVar, probeIVar,
        hostGIVar,globIVar, slotGIVar, putGIVar, superviseIVar)
import Control.Parallel.HdpH.Internal.Location (NodeId, dbgStaticTab)
import Control.Parallel.HdpH.Internal.Scheduler
       (RTS,liftThreadM, liftSparkM, liftCommM, liftIO,
        {-schedulerID, -} mkThread, execThread, sendPUSH)
import qualified Control.Parallel.HdpH.Internal.Scheduler as Scheduler (run_)
import Control.Parallel.HdpH.Internal.Sparkpool (putSpark)
import Control.Parallel.HdpH.Internal.Threadpool (putThread, putThreads)
import Control.Parallel.HdpH.Internal.Type.Par
        (ParM(Par), unPar, Thread(Atom), SupervisedSpark(..), Scheduling(..),CurrentLocation(..))
import qualified Control.Parallel.HdpH.Internal.Type.Par  as Par (declareStatic)
import Control.Parallel.HdpH.Internal.Type.GRef (taskHandle)
import Data.IORef
import Control.Concurrent.MVar

-----------------------------------------------------------------------------
-- Static declaration

declareStatic :: StaticDecl
declareStatic = mconcat
  [Control.Parallel.HdpH.Closure.declareStatic,
   Par.declareStatic,
   declare $(static 'rput_abs),
   declare $(static 'spawn_abs)]


-----------------------------------------------------------------------------
-- abstract IVars and GIVars

newtype IVar a = IVar (IVar.IVar RTS a)

newtype GIVar a = GIVar (IVar.GIVar RTS a)
                  deriving (Eq, Ord, NFData, Binary)

-- Show instance (mainly for debugging)
instance Show (GIVar a) where
  showsPrec _ (GIVar gv) = showString "GIVar:" . shows gv

at :: GIVar a -> NodeId
at (GIVar gv) = hostGIVar gv

-----------------------------------------------------------------------------
-- abstract runtime system

-- Module HdpH exports a monad 'RTS' which encapsulates all Haskell-level
-- runtime system functionality. It offers operations to run RTS computations
-- (which will mostly be generated by 'runPar', but can also be lifted IO
-- computations), performing parallel machine setup and initialisation
-- behind the scenes. Every RTS computation must issue the action 'shutdownRTS'
-- on exactly one node before terminating.

-- Eliminate the RTS monad down to IO by running the given 'action';
-- aspects of the RTS's behaviour are controlled by the respective parameters
-- in the 'conf' argument.
runRTS_ :: RTSConf -> RTS () -> IO ()
runRTS_ = Scheduler.run_

-- Return True iff this node is the root (or main) node.
isMainRTS :: RTS Bool
isMainRTS = liftCommM Comm.isMain

-- Initiate RTS shutdown (which will print stats, given the right debug level).
shutdownRTS :: RTS ()
shutdownRTS = liftCommM Comm.shutdown

-- Print global Static table to stdout, one entry a line.
printStaticTable :: RTS ()
printStaticTable =
  liftIO $ mapM_ putStrLn $
    "Static Table:" : map ("  " ++) showStaticTable


-----------------------------------------------------------------------------
-- Par monad, based on ideas from
--   [1] Cloaessen "A Poor Man's Concurrency Monad", JFP 9(3), 1999.
--   [2] Marlow et al. "A monad for deterministic parallelism". Haskell 2011.

-- TODO: The use of the Par monad is rather close to Bill Harrison's
--       'resumption monads' [3]. Try to rewrite the definitions here
--       and in Control.Parallel.HdpH.Internal.Type.Par to make Par a resumption monad.
--       [3] Harrison "The Essence of Multitasking". AMAST 2006.
-- ANSWER: Would actually need to make Par into a reactive resumption monad,
--         which is not very different from the current continuation style.

-- 'Par' is a type synonym hiding the RTS parameter; a newtype would be
-- nicer but the resulting wrapping and unwrapping destroys readability
-- (if it is possible at all).

type Par = ParM RTS

instance Functor Par where
    fmap f m = Par $ \ c -> unPar m (c . f)

-- The Monad instance is where we differ from Control.Monad.Cont,
-- the difference being the use of strict application ($!).
-- TODO: Test whether strictness is still necessary; it was prior to v0.3.0
--       to ensure thread actions were actually executed; differences
--       in the action type may render it unnecessary.
-- ANSWER: Strictness is not necessary now, but it does increase performance
--         marginally.
instance Monad Par where
    return a = Par $ \ c -> c $! a
    m >>= k  = Par $ \ c -> unPar m $ \ a -> unPar (k $! a) c


-- Eliminate the 'Par' monad layer by converting the given 'Par' action 'p'
-- into an RTS action (which should then be executed on one node of the
-- parallel machine).
runPar :: Par a -> RTS a
runPar p = do -- create an empty MVar expecting the result of action 'p'
              res <- liftIO $ newEmptyMVar

              -- fork 'p', combined with a write to above MVar;
              -- note that the starter thread (ie the 'fork') runs outwith
              -- any scheduler (and terminates quickly); the forked action
              -- (ie. 'p >>= ...') runs in a scheduler, however.
              execThread $ mkThread $ fork (p >>= io . putMVar res)

              -- block waiting for result
              liftIO $ takeMVar res


-- Convenience: combines 'runPar' and 'runRTS_' (inclouding shutdown).
-- TODO: There appear to be races during shutdown which can cause the
--       system to hang.
runParIO_ :: RTSConf -> Par () -> IO ()
runParIO_ conf p = runRTS_ conf $ do isMain <- isMainRTS
                                     when isMain $ do
                                       -- print Static table
                                       when (dbgStaticTab <= debugLvl conf)
                                         printStaticTable
                                       runPar p
                                       shutdownRTS


-- Convenience: variant of 'runParIO_' which does return a result
-- (on the main node; all other nodes return Nothing).
runParIO :: RTSConf -> Par a -> IO (Maybe a)
runParIO conf p = do res <- newIORef Nothing
                     runParIO_ conf (p >>= io . writeIORef res . Just)
                     readIORef res


-----------------------------------------------------------------------------
-- operations in the Par monad

-- TODO: Implement 'yield :: Par ()'; semantics like Control.Concurrent.yield

-- thread termination
done :: Par a
done = Par $ \ _c -> Atom (return Nothing)

-- lifting RTS into the Par monad (really a monadic map); don't export
atom :: RTS a -> Par a
atom m = Par $ \ c -> Atom (return . Just . c =<< m)

-- current node
myNode :: Par NodeId
myNode = atom $ liftCommM $ Comm.myNode

-- all nodes of the parallel machine
allNodes :: Par [NodeId]
allNodes = atom $ liftCommM $ Comm.allNodes

-- lifting an IO action into the Par monad
io :: IO a -> Par a
io = atom . liftIO

-- evaluate to weak head normal form
eval :: a -> Par a
eval x = atom (x `seq` return x)

-- force to normal form
force :: (NFData a) => a -> Par a
force x = atom (x `deepseq` return x)

-- thread creation
fork :: Par () -> Par ()
fork = atom . liftThreadM . putThread . mkThread

-- create a spark (ie. put it in the spark pool)
spark :: Closure (Par ()) -> Par ()
spark clo = atom (liftSparkM $ putSpark (Right clo))

sparkSupervised :: Closure (SupervisedSpark RTS) -> Par ()
sparkSupervised clo = atom (liftSparkM $ putSpark (Left clo))

-- push a spark to the node given node (for eager execution)
pushTo :: Closure (Par ()) -> NodeId -> Par ()
pushTo clo node = atom $ sendPUSH (Right clo) node

-----------------------
-- spawn operations
mkSpawnedClo :: Closure (Par (Closure a)) -> Par (Closure (Par ()), IVar (Closure a), GIVar (Closure a))
mkSpawnedClo clo = do
  v <- new
  gv <- glob v
  return ($(mkClosure [| spawn_abs (clo, gv) |]),v,gv)

spawn :: Closure (Par (Closure a)) -> Par (IVar (Closure a))
spawn clo = do
  (clo',v,_) <- mkSpawnedClo clo
  spark clo'
  return v

spawnAt :: Closure (Par (Closure a)) -> NodeId -> Par (IVar (Closure a))
spawnAt clo target = do
  (clo',v,_) <- mkSpawnedClo clo
  pushTo clo' target
  return v

-----------------------
-- spawn operations

mkSupervisedSpawnedClo :: forall a . Closure (Par (Closure a)) -> Scheduling -> CurrentLocation -> Par (Closure (Par ()), IVar (Closure a), GIVar (Closure a))
mkSupervisedSpawnedClo clo howScheduled currentLocation = do
  v <- new
  gv@(GIVar gv') <- glob v
  supV@(IVar supervisedV) <- newSupervised $(mkClosure [| spawn_abs (clo, gv) |])
  io $ superviseIVar supervisedV (slotGIVar gv') -- insert newly supervised IVar in place of placeholder IVar
  return ($(mkClosure [| spawn_abs (clo, gv) |]),supV,gv)
 where
  newSupervised :: Closure (Par ()) -> Par (IVar a)
  newSupervised clo' = IVar <$> atom (liftIO $ newSupervisedIVar clo' howScheduled currentLocation)

mkSupervisedSpark :: Closure (ParM m ()) -> GIVar a -> Closure (SupervisedSpark m)
mkSupervisedSpark closure (GIVar gv) = toClosure
  SupervisedSpark
    { clo = closure
    , remoteRef = taskHandle gv
    , thisReplica = 0
    }

supervisedSpawn :: Closure (Par (Closure a)) -> Par (IVar (Closure a))
supervisedSpawn clo = do
  here' <- myNode
  (clo',v,gv) <- mkSupervisedSpawnedClo clo Sparked (OnNode here')
  let t = mkSupervisedSpark clo' gv
  sparkSupervised t
  return v

supervisedSpawnAt :: Closure (Par (Closure a)) -> NodeId -> Par (IVar (Closure a))
supervisedSpawnAt clo target = do
  (clo',v,_) <- mkSupervisedSpawnedClo clo Pushed (OnNode target)
  pushTo clo' target
  return v

{-# INLINE spawn_abs #-}
spawn_abs :: (Closure (Par (Closure a)), GIVar (Closure a)) -> Par ()
spawn_abs (clo, gv) = unClosure clo >>= rput gv

-- End of FT primitives
-----------------------

-- IVar creation
new :: Par (IVar a)
new = IVar <$> atom (liftIO $ newIVar)

-- write to IVar (but don't force any evaluation)
put :: IVar a -> a -> Par ()
put (IVar v) a = atom $ liftIO (putIVar v a) >>=
                        liftThreadM . putThreads

-- blocking read from IVar
get :: IVar a -> Par a
get (IVar v) = Par $ \ c -> Atom $ liftIO (getIVar v c) >>=
                                   maybe (return Nothing) (return . Just . c)

-- polling the value of an IVar; non-blocking
poll :: IVar a -> Par (Maybe a)
poll (IVar v) = atom $ liftIO (pollIVar v)

-- probing whether an IVar is full; non-blocking
probe :: IVar a -> Par Bool
probe (IVar v) = atom $ liftIO (probeIVar v)

probeIO :: IVar a -> IO Bool
probeIO (IVar v) = (probeIVar v)

-- globalise IVar (of Closure type)
glob :: IVar (Closure a) -> Par (GIVar (Closure a))
glob (IVar v) = GIVar <$> atom (liftIO $ globIVar v)

-- remote write to global IVar (of Closure type)
rput :: GIVar (Closure a) -> Closure a -> Par ()
rput gv clo = do
  me <- myNode
  if me == (at gv)
   then rput_abs (gv,clo)
   else pushTo $(mkClosure [| rput_abs (gv, clo) |]) (at gv)

-- write to locally hosted global IVar; don't export
{-# INLINE rput_abs #-}
rput_abs :: (GIVar (Closure a), Closure a) -> Par ()
rput_abs (GIVar gv, clo) = atom $ liftIO (putGIVar gv clo) >>=
                                  liftThreadM . putThreads

parseOpts :: [String] -> (RTSConf, Int, [String])
parseOpts args = go (defaultRTSConf, 0, args) where
  go :: (RTSConf, Int, [String]) -> (RTSConf, Int, [String])
  go (conf, seed, [])   = (conf, seed, [])
  go (conf, seed, s:ss) =
   case stripPrefix "-rand=" s of
   Just s  -> go (conf, read s, ss)
   Nothing ->
    case stripPrefix "-d" s of
    Just s  -> go (conf { debugLvl = read s }, seed, ss)
    Nothing ->
     case stripPrefix "-scheds=" s of
     Just s  -> go (conf { scheds = read s }, seed, ss)
     Nothing ->
      case stripPrefix "-wakeup=" s of
      Just s  -> go (conf { wakeupDly = read s }, seed, ss)
      Nothing ->
       case stripPrefix "-hops=" s of
       Just s  -> go (conf { maxHops = read s }, seed, ss)
       Nothing ->
        case stripPrefix "-maxFish=" s of
        Just s  -> go (conf { maxFish = read s }, seed, ss)
        Nothing ->
         case stripPrefix "-minSched=" s of
         Just s  -> go (conf { minSched = read s }, seed, ss)
         Nothing ->
          case stripPrefix "-minNoWork=" s of
          Just s  -> go (conf { minFishDly = read s }, seed, ss)
          Nothing ->
           case stripPrefix "-numProcs=" s of
           Just s  -> go (conf { numProcs = read s }, seed, ss)
           Nothing ->
            case stripPrefix "-maxNoWork=" s of
            Just s  -> go (conf { maxFishDly = read s }, seed, ss)
            Nothing ->
             case stripPrefix "-keepAliveFreq=" s of
              Just s  -> go (conf { keepAliveFreq = read s }, seed, ss)
              Nothing ->
                case stripPrefix "-killAt=" s of
                 Just s -> if s == "-1"
                           then (conf, seed, ss)
                           else go (conf { deathTiming = deathTimings s }, seed, ss)
                 Nothing ->
                  case stripPrefix "-chaosMonkey" s of
                    Just "" -> go (conf { chaosMonkey = True }, seed, ss)
                    Nothing -> (conf, seed, s:ss)

deathTimings :: String -> [Int]
deathTimings s  = map read (splitOn "," s)

chaosMonkeyUnitTest :: (Eq a, Show a) => RTSConf -> String -> a -> Par a -> IO ()
chaosMonkeyUnitTest conf label expected f = do
    let tests = TestList $ [runTest]
    void $ runTestTT tests
  where
    runTest :: Test
    runTest =
      let chaosMonkeyConf = conf {chaosMonkey = True, maxFish = 10 , minSched = 11}
          test' = do
           result <- evaluate =<< runParIO chaosMonkeyConf f
           case result of
             Nothing -> assert True
             Just x -> putStrLn (label++" result: "++show x) >>
                       assertEqual label x expected
      in TestLabel label (TestCase test')
