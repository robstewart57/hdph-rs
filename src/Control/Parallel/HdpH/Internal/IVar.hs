-- Local and global IVars
--
-- Author: Patrick Maier
{-# LANGUAGE RankNTypes
           , ExistentialQuantification
           , BangPatterns #-}

-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.IVar
  ( -- * local IVar type
    IVar,       -- synonym: IVar m a = IORef <IVarContent m a>
    IVarContent(..),

    -- * operations on local IVars
    newIVar,    -- :: IO (IVar m a)
    newSupervisedIVar, -- :: Closure (ParM m ()) -> Scheduling -> CurrentLocation -> IO (IVar m a)
    superviseIVar, -- TODO: document
    putIVar,    -- :: IVar m a -> a -> IO [Thread m]
    getIVar,    -- :: IVar m a -> (a -> Thread m) -> IO (Maybe a)
    pollIVar,   -- :: IVar m a -> IO (Maybe a)
    probeIVar,  -- :: IVar m a -> IO Bool

    -- * global IVar type
    GIVar,      -- synonym: GIVar m a = GRef (IVar m a)

    -- * operations on global IVars
    globIVar,   -- :: Int -> IVar m a -> IO (GIVar m a)
    hostGIVar,  -- :: GIVar m a -> NodeId
    slotGIVar,  -- :: GIVar m a -> Integer
    putGIVar,   -- :: Int -> GIVar m a -> a -> IO [Thread m]

    -- * check state of empty IVars
    isNewestReplica,  -- :: TaskRef -> Int -> IO (Maybe Bool)
    locationOfTask,   -- :: TaskRef -> IO (Maybe CurrentLocation)
    vulnerableEmptyFutures, -- :: NodeId -> IO [IVar m a]

    -- * Update state of empty IVars
    taskInTransition, -- :: TaskRef -> NodeId -> NodeId -> IO ()
    taskOnNode,       -- :: TaskRef -> NodeId -> IO ()
    replicateSpark,     -- :: IVar m a -> IO (Maybe (SupervisedSpark m))
    replicateThread     -- :: IVar m a -> IO (Maybe (Closure (ParM m ()))

  ) where

import Prelude
import Data.Functor ((<$>))
import Data.IORef (IORef, newIORef, readIORef, atomicModifyIORef,writeIORef)
import qualified Data.IntMap.Strict as Map
import Data.Maybe (isJust,fromMaybe,fromJust,isNothing)
import Data.Binary
import Control.Monad (filterM,unless,void)
import Control.Concurrent (forkIO)
import Control.DeepSeq (NFData(rnf))
import Control.Parallel.HdpH.Internal.Location
       (NodeId, debug, dbgGIVar, dbgIVar,dbgGRef,myNode)
import Control.Parallel.HdpH.Internal.Type.GRef
import Control.Parallel.HdpH.Closure (Closure)
import Control.Parallel.HdpH.Internal.Type.Par
import System.IO.Unsafe (unsafePerformIO)
import Control.Parallel.HdpH.Internal.Misc (atomicWriteIORef)

-----------------------------------------------------------------------------
-- type of local IVars

-- An IVar is a mutable reference to either a value or a list of blocked
-- continuations (waiting for a value);
-- the parameter 'm' abstracts a monad (cf. module HdpH.Internal.Type.Par).
type IVar m a = IORef (IVarContent m a)

data IVarContent m a = Full a
                     | Empty
  { blockedThreads :: [a -> Thread m]
  , taskLocalState :: Maybe (SupervisedTaskState m)
  }


-----------------------------------------------------------------------------
-- operations on local IVars, borrowing from
--    [1] Marlow et al. "A monad for deterministic parallelism". Haskell 2011.

newSupervisedIVar :: Closure (ParM m ()) -> Scheduling -> CurrentLocation -> IO (IVar m a)
newSupervisedIVar closure howScheduled taskLocation = do
  let localSt = SupervisedTaskState
          { task = closure
          , scheduling = howScheduled
          , location = taskLocation
          , newestReplica = 0
          }
  newIORef $ Empty { blockedThreads = [] , taskLocalState = Just localSt }


-- | Create a new unsupervised empty IVar.
newIVar :: IO (IVar m a)
newIVar = newIORef $
  Empty { blockedThreads = [] , taskLocalState = Nothing }

-- Write 'x' to the IVar 'v' and return the list of blocked threads.
-- Unlike [1], multiple writes fail silently (ie. they do not change
-- the value stored, and return an empty list of threads).
putIVar :: IVar m a -> a -> IO [Thread m]
putIVar v x = do
  e <- readIORef v
  case e of
    Full _    -> do debug dbgIVar $ "Put to full IVar"
                    return []
    Empty _ _ -> do maybe_ts <- atomicModifyIORef v fill_and_unblock
                    case maybe_ts of
                      Nothing -> do debug dbgIVar $ "Put to full IVar"
                                    return []
                      Just ts -> do debug dbgIVar $
                                      "Put to empty IVar; unblocking " ++
                                      show (length ts) ++ " threads"
                                    return ts
      where
     -- fill_and_unblock :: IVarContent m a ->
     --                       (IVarContent m a, Maybe [Thread m])
        fill_and_unblock e' =
          case e' of
            Full _     -> (e',      Nothing)
            Empty cs _ -> (Full x,  Just $ map ($ x) cs)

-- Read from the given IVar 'v' and return the value if it is full.
-- Otherwise add the given continuation 'c' to the list of blocked
-- continuations and return nothing.
getIVar :: IVar m a -> (a -> Thread m) -> IO (Maybe a)
getIVar v c = do
  e <- readIORef v
  case e of
    Full x    -> do return (Just x)
    Empty _ _ -> do maybe_x <- atomicModifyIORef v get_or_block
                    case maybe_x of
                      Just _  -> do return maybe_x
                      Nothing -> do debug dbgIVar $ "Blocking on IVar"
                                    return maybe_x
      where
        -- get_or_block :: IVarContent m a -> (IVarContent m a, Maybe a)
        get_or_block e' =
          case e' of
            Full x     ->  (e',              Just x)
            Empty cs st -> (Empty (c:cs) st, Nothing)


-- Poll the given IVar 'v' and return its value if full, Nothing otherwise.
-- Does not block.
pollIVar :: IVar m a -> IO (Maybe a)
pollIVar v = do
  e <- readIORef v
  case e of
    Full x    -> return (Just x)
    Empty _ _ -> return Nothing

-- Probe whether the given IVar is full, returning True if it is.
-- Does not block.
probeIVar :: IVar m a -> IO Bool
probeIVar v = isJust <$> pollIVar v



-----------------------------------------------------------------------------
-- type of global IVars; instances mostly inherited from global references

-- A global IVar is a global reference to an IVar; 'm' abstracts a monad.
-- NOTE: The HdpH interface will restrict the type parameter 'a' to
--       'Closure b' for some type 'b', but but the type constructor 'GIVar'
--       does not enforce this restriction.
type GIVar m a = GRef (IVar m a)

-----------------------------------------------------------------------------
-- operations on global IVars

-- Returns node hosting given global IVar.
hostGIVar :: GIVar m a -> NodeId
hostGIVar = at

slotGIVar :: GIVar m a -> Int
slotGIVar = slot

-- Globalise the given IVar;
globIVar :: IVar m a -> IO (GIVar m a)
globIVar v = do
  gv <- globalise v
  debug dbgGIVar $ "New global IVar " ++ show gv
  return gv

superviseIVar :: IVar m a -> Int -> IO ()
superviseIVar newV i = do
  supervise newV i
  debug dbgGIVar $ "IVar supervised at slot " ++ show i

-- Write 'x' to the locally hosted global IVar 'gv', free 'gv' and return
-- the list of blocked threads. Like putIVar, multiple writes fail silently
-- (as do writes to a dead global IVar);
putGIVar :: GIVar m a -> a -> IO [Thread m]
putGIVar gv x = do
  debug dbgGIVar $ "Put to global IVar " ++ show gv
  ts <- withGRef gv (\ v -> putIVar v x) (return [])
  free gv    -- free 'gv' (eventually)
  return ts


------------------------------
-- direct access to registry.

lookupIVar :: TaskRef -> IO (Maybe (IVar m a))
lookupIVar taskRef = do
    reg <- table <$> readIORef regRef
    return $ Map.lookup (slotT taskRef) reg

-- | This is called when notification of a dead node has been received.
--   It looks through the registry, identifying all empty IVars whose associated
--   task may have sank with sunken node. The criteria for selection is:
--
--   1. The location of the task was either deadNode in (OnNode deadNode) or
--      one of nodes in (InTransition from to).
--
--   2. The IVar is empty i.e. yet to be filled by the task.
vulnerableEmptyFutures :: NodeId -> IO [(Int,IVar m a)]
vulnerableEmptyFutures deadNode = do
    reg <- table <$> readIORef regRef
    let ivars = Map.toList reg
    atRiskIVars <- filterM taskAtRisk  ivars
    filterM (\(_,i) -> not <$> probeIVar i) atRiskIVars
  where
    taskAtRisk :: (Int,IVar m a) -> IO Bool
    taskAtRisk (_,v) = do
      e <- readIORef v
      case e of
        Full _ -> return False
        Empty _ maybe_st -> do
          if isNothing maybe_st
           then return False
           else do
            let st = fromJust (taskLocalState e)
            case location st of
              OnNode node -> return (node == deadNode)
              InTransition from to ->
                return $ (from == deadNode) || (to == deadNode)

-- | Creates a replica of the task that will fill the 'IVar'.
--   It extracts the closure within the Empty IVar state. It
--   then increments the 'newestReplica' replica number within the
--   IVar state, and attaches this sequence number to the duplicate
--   task. The supervisor only permits subsequent 'FISH' requests
--   that refer to this task, so that book keeping for a future is not
--   corrupted by nonsensical sequences of book keeping updates.
replicateSpark :: (Int,IVar m a) -> IO (Maybe (SupervisedSpark m))
replicateSpark (indexRef,v) = do
  me <- myNode
  atomicModifyIORef v $ \e ->
    case e of
      Full _ -> (e,Nothing) -- TODO turn to debug message
                -- cannot duplicate task, IVar full, task garbage collected
      Empty b maybe_st ->
        let ivarSt = fromMaybe
             (error "cannot duplicate non-supervised task")
             maybe_st
            newTaskLocation =  OnNode me
            newReplica = (newestReplica ivarSt) + 1
            supervisedTask = SupervisedSpark
              { clo = task ivarSt
              , thisReplica = newReplica
              , remoteRef = TaskRef indexRef me
              }
            newIVarSt = ivarSt { newestReplica = newReplica , location = newTaskLocation}
        in (Empty b (Just newIVarSt),Just supervisedTask)

replicateThread :: (Int,IVar m a) -> IO (Maybe (Closure (ParM m ())))
replicateThread (indexRef,v) = do
  me <- myNode
  atomicModifyIORef v $ \e ->
    case e of
      Full _ -> (e,Nothing) -- TODO turn to debug message
                -- cannot duplicate task, IVar full, task garbage collected
      Empty b maybe_st ->
        let ivarSt = fromMaybe
             (error "cannot duplicate non-supervised task")
             maybe_st
            newTaskLocation =  OnNode me
            newReplica = (newestReplica ivarSt) + 1
            threadCopy = task ivarSt
            newIVarSt = ivarSt { newestReplica = newReplica , location = newTaskLocation}
        in (Empty b (Just newIVarSt),Just threadCopy)


-- | Updates location of empty IVar to 'OnNode'.
taskOnNode :: TaskRef -> NodeId -> IO ()
taskOnNode taskRef newNode = do
  maybe_ivar <- lookupIVar taskRef
  let v =  fromMaybe
              (error "taskInTransition: Local IVar not found")
              maybe_ivar
  updateLocation v (OnNode newNode)

-- | Updates location of empty IVar to 'InTransition'.
taskInTransition :: TaskRef -> NodeId -> NodeId -> IO ()
taskInTransition taskRef from to = do
  maybe_ivar <- lookupIVar taskRef
  let v =  fromMaybe
              (error "taskInTransition: Local IVar not found")
              maybe_ivar
  updateLocation v (InTransition from to)

-- | Lookup location book keeping for an empty IVar.
--   Returns 'Nothing' is the IVar is full.
locationOfTask :: TaskRef -> IO (Maybe CurrentLocation)
locationOfTask taskRef = do
  maybe_ivar <- lookupIVar taskRef
  let v =  fromMaybe
              (error "taskInTransition: Local IVar not found")
              maybe_ivar
  e <- readIORef v
  case e of
    Full{} -> return Nothing -- IVar full, no longer maintains location
    Empty _ maybe_st -> do
      return (if isNothing maybe_st
              then Nothing
              else Just $ location (fromJust maybe_st))

      if isNothing maybe_st
       then return Nothing -- not supervised
       else return $ Just $ location (fromJust maybe_st)

-- | Checks in the local registry. Returns 'Just True' iff the
--   sequence number of the task matches the 'newestReplica' in
--   the IVar state. Returns 'Just False' if lower than 'newestReplica'.
--   Returns 'Nothing' if the IVar has been filled by a copy of the task.
isNewestReplica :: TaskRef -> Int -> IO (Maybe Bool)
isNewestReplica taskRef taskSeq = do
    maybe_ivar <- lookupIVar taskRef
    let v =  fromMaybe
                (error "isNewestReplica: Local IVar not found")
                maybe_ivar
    e <- readIORef v
    case e of
      Full{} -> return Nothing -- IVar full
      Empty _ maybe_st -> do
        if isNothing maybe_st
         then return Nothing -- task not supervise
         else do
           return $ Just $ taskSeq == newestReplica (fromJust maybe_st)

-- not exposed.
updateLocation ::  IVar m a -> CurrentLocation -> IO ()
updateLocation v newLoc = do
    atomicModifyIORef v $ \e -> do
    case e of
      Full _ -> error "updating task location of full IVar"
      Empty _ maybe_st ->
        let st    = fromMaybe
                      (error "trying to update location of unsupervised task")
                      maybe_st
            newSt = Just $ updateLocationSt st newLoc
        in (e { taskLocalState = newSt },())
  where
    updateLocationSt :: SupervisedTaskState m -> CurrentLocation -> SupervisedTaskState m
    updateLocationSt st loc = st { location = loc }


-----------------------------------------------------------------------------
-- registry for global references

-- Registry, comprising of the most recently allocated slot and a table
-- mapping slots to objects (wrapped in an existential type).
data GRefReg m a = GRefReg { lastSlot :: !Int,
                             table    :: Map.IntMap (IVar m a) }

regRef :: IORef (GRefReg m a)
regRef = unsafePerformIO $
           newIORef $ GRefReg { lastSlot = 0, table = Map.empty }
{-# NOINLINE regRef #-}   -- required to protect unsafePerformIO hack


-----------------------------------------------------------------------------
-- Key facts about global references
--
-- * A global reference is a globally unique handle naming a Haskell value;
--   the type of the value is reflected in a phantom type argument to the
--   type of global reference, similar to the type of stable names.
--
-- * The link between a global reference and the value it names is established
--   by a registry mapping references to values. The registry mapping a
--   global reference resides on the node hosting its value. All operations
--   involving the reference must be executed on the hosting node; the only
--   exception is the function 'at', projecting a global reference to its
--   hosting node.
--
-- * The life time of a global reference is not linked to the life time of
--   the named value, and vice versa. One consequence is that global
--   references can never be re-used, unlike stable names.
--
-- * For now, global references must be freed explicitly (from the map
--   on the hosting node). This could (and should) be changed by using
--   weak pointers and finalizers.


-----------------------------------------------------------------------------
-- global references (abstract outwith this module)
-- NOTE: Global references are hyperstrict.

-- Constructs a 'GRef' value of a given node ID and slot (on the given node);
-- ensures the resulting 'GRef' value is hyperstrict;
-- this constructor is not to be exported.
mkGRef :: NodeId -> Int -> GRef a
mkGRef node i = rnf node `seq` rnf i `seq` GRef { slot = i, at = node }

instance Eq (GRef a) where
  ref1 == ref2 = slot ref1 == slot ref2 && at ref1 == at ref2

instance Ord (GRef a) where
  compare ref1 ref2 = case compare (slot ref1) (slot ref2) of
                        LT -> LT
                        GT -> GT
                        EQ -> compare (at ref1) (at ref2)


-- Show instance (mainly for debugging)
instance Show (GRef a) where
  showsPrec _ ref =
    showString "GRef:" . shows (at ref) . showString "." . shows (slot ref)

instance NFData (GRef a)  -- default instance suffices (due to hyperstrictness)

instance Binary (GRef a) where
  put ref = Data.Binary.put (at ref) >>
            Data.Binary.put (slot ref)
  get = do node <- Data.Binary.get
           i <- Data.Binary.get
           return $ mkGRef node i  -- 'mkGRef' ensures result is hyperstrict


-----------------------------------------------------------------------------
-- predicates on global references

-- Monadic projection; True iff the current node hosts the object refered
-- to by the given global 'ref'.
isLocal :: GRef a -> IO Bool
isLocal ref = (at ref ==) <$> myNode


-- Checks if a locally hosted global 'ref' is live.
-- Aborts with an error if 'ref' is a not hosted locally.
isLive :: GRef a -> IO Bool
isLive ref = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.isLive: " ++ show ref ++ " not local"
  reg <- readIORef regRef
  return $ Map.member (slot ref) (table reg)


-----------------------------------------------------------------------------
-- updating the registry

-- Registers its argument as a global object (hosted on the current node),
-- returning a fresh global reference. May block when attempting to access
-- the registry.
globalise :: IVar m a -> IO (GRef (IVar m a))
globalise x = do
  node <- myNode
  ref <- atomicModifyIORef regRef (createEntry x node)
  debug dbgGRef $ "GRef.globalise " ++ show ref
  return ref

supervise :: IVar m a -> Int -> IO ()
supervise newV i = do
  node <- myNode
  atomicModifyIORef regRef (promoteToSupervisedIVar newV node i)

-- Asynchronously frees a locally hosted global 'ref'; no-op if 'ref' is dead.
-- Aborts with an error if 'ref' is a not hosted locally.
free :: GRef a -> IO ()
free ref = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.free: " ++ show ref ++ " not local"
  void $ forkIO $ do debug dbgGRef $ "GRef.free " ++ show ref
                     atomicModifyIORef regRef (deleteEntry $ slot ref)
  return ()


-- Frees a locally hosted global 'ref'; no-op if 'ref' is dead.
-- Aborts with an error if 'ref' is a not hosted locally.
freeNow :: GRef a -> IO ()
freeNow ref = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.freeNow: " ++ show ref ++ " not local"
  debug dbgGRef $ "GRef.freeNow " ++ show ref
  atomicModifyIORef regRef (deleteEntry $ slot ref)


-- Create new entry in 'reg' (hosted on 'node') mapping to 'val'; not exported
createEntry :: IVar m a -> NodeId -> GRefReg m a -> (GRefReg m a, GRef (IVar m a))
createEntry val node reg =
  ref `seq` (reg', ref) where
    newSlot = lastSlot reg + 1
    ref = mkGRef node newSlot  -- 'seq' above forces hyperstrict 'ref' to NF
    reg' = reg { lastSlot = newSlot,
                 table    = Map.insert newSlot val (table reg) }

promoteToSupervisedIVar :: IVar m a -> NodeId -> Int -> GRefReg m a -> (GRefReg m a,())
promoteToSupervisedIVar newVal node !slot reg =
  (reg', ()) where
    reg' = reg { table = Map.insert slot newVal (table reg) }

-- Delete entry 'slot' from 'reg'; not exported
deleteEntry :: Int -> GRefReg m a -> (GRefReg m a, ())
deleteEntry ivarSlot reg =
  (reg { table = Map.delete ivarSlot (table reg) }, ())

-----------------------------------------------------------------------------
-- Dereferencing global refs

-- Attempts to dereference a locally hosted global 'ref' and apply 'action'
-- to the refered-to object; executes 'dead' if that is not possible (ie.
-- 'dead' acts as an exception handler) because the global 'ref' is dead.
-- Aborts with an error if 'ref' is a not hosted locally.
withGRef :: GRef (IVar m a) -> (IVar m a -> IO b) -> IO b -> IO b
withGRef ref action dead = do
  refIsLocal <- isLocal ref
  unless refIsLocal $
    error $ "HdpH.Internal.GRef.withGRef: " ++ show ref ++ " not local"
  reg <- readIORef regRef
  case Map.lookup (slot ref) (table reg) of
    Nothing      -> do debug dbgGRef $ "GRef.withGRef " ++ show ref ++ " dead"
                       dead
    Just (x) -> do action x -- (unsafeCoerce x)
                    -- see below for an argument why unsafeCoerce is safe here


-------------------------------------------------------------------------------
-- Notes on the design of the registry
--
-- * A global reference is represented as a pair consisting of the ID
--   of the hosting node together with its 'slot' in the registry on
--   that node. The slot is an unbounded integer so that there is an
--   infinite supply of slots. (Slots can't be re-used as there is no
--   global garbage collection of global references.)
--
-- * The registry maps slots to values, which are essentially untyped
--   (the type information being swallowed by an existential wrapper).
--   However, the value type information is not lost as it can be
--   recovered from the phantom type argument of its global reference.
--   In fact, the function 'withGRef' super-imposes a reference's phantom
--   type on to its value via 'unsafeCoerce'. The reasons why this is safe
--   are laid out below.


-- Why 'unsafeCoerce' in safe in 'withGRef':
--
-- * Global references can only be created by the function 'globalise'.
--   Whenever this function generates a global reference 'ref' of type
--   'GRef t' it guarantees that 'ref' is globally fresh, ie. its
--   representation does not exist any where else in the system, nor has
--   it ever existed in the past. (Note that freshness relies on the
--   assumption that node IDs themselves are fresh, which is relevant
--   in case nodes can leave and join dynmically.)
--
-- * A consequence of global freshness is that there is a functional relation
--   from representations to phantom types of global references. For all
--   global references 'ref1 :: GRef t1' and 'ref2 :: GRef t2',
--   'at ref1 == at ref2 && slot ref1 == slot ref2' implies the identity
--   of the phantom types t1 and t2.
--
-- * Thus, we can safely super-impose (using 'unsafeCoerce') the phantom type
--   of a global reference on to its value.
