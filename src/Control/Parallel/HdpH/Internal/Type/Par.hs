

-- Par monad and thread representation; types
--
-- Visibility: HpH.Internal.{IVar,Sparkpool,Threadpool,Scheduler}
-- Author: Patrick Maier <P.Maier@hw.ac.uk>
-- Created: 28 Sep 2011
--

{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-----------------------------------------------------------------------------

module Control.Parallel.HdpH.Internal.Type.Par
  ( -- * Par monad, threads and sparks
    ParM(..),
    Thread(..),
    CurrentLocation(..),
    Task,SupervisedSpark(..),
    SupervisedTaskState(..),Scheduling(..),
    taskSupervised,declareStatic
  ) where

import Prelude
import Control.Parallel.HdpH.Internal.Location (NodeId)
import Control.Parallel.HdpH.Internal.Type.GRef (TaskRef)
import Data.Monoid (mconcat)
import Control.Parallel.HdpH.Closure
       (Closure,ToClosure(..),StaticDecl,StaticToClosure,
        declare,staticToClosure,here)
import Data.Binary (Binary,put,get)


declareStatic :: StaticDecl
declareStatic = mconcat
  [declare (staticToClosure :: forall a . StaticToClosure (SupervisedSpark a))]

-----------------------------------------------------------------------------
-- Par monad, based on ideas from
--   [1] Claessen "A Poor Man's Concurrency Monad", JFP 9(3), 1999.
--   [2] Marlow et al. "A monad for deterministic parallelism". Haskell 2011.

-- 'ParM m' is a continuation monad, specialised to the return type 'Thread m';
-- 'm' abstracts a monad encapsulating the underlying state.
newtype ParM m a = Par { unPar :: (a -> Thread m) -> Thread m }

-- A thread is determined by its actions, as described in this data type.
-- In [2] this type is called 'Trace'.
newtype Thread m = Atom (m (Maybe (Thread m)))  -- atomic action (in monad 'm')
                                                -- result is next action, maybe

data SupervisedSpark m = SupervisedSpark
  {
    clo    :: Closure (ParM m ())
  , remoteRef :: TaskRef
  , thisReplica :: Int
  }

instance Binary (SupervisedSpark m) where
    put (SupervisedSpark closure ref thisSeq) =
                             Data.Binary.put closure >>
                             Data.Binary.put ref >>
                             Data.Binary.put thisSeq
    get = do
      closure <- Data.Binary.get
      ref <- Data.Binary.get
      thisSeq <- Data.Binary.get
      return $ SupervisedSpark closure ref thisSeq

type Task m = Either (Closure (SupervisedSpark m)) (Closure (ParM m ()))

taskSupervised :: Task m -> Bool
taskSupervised (Left _) = True
taskSupervised (Right _) = False


-- |Local representation of a spark for the supervisor
data SupervisedTaskState m = SupervisedTaskState
  {
    -- | The copy of the task that will need rescheduled,
    --   in the case when it has not been evaluated.
    task :: Closure (ParM m ())

    -- | Used by the spark supervisor. Used to decide
    --   whether to put the spark copy in the local
    --   sparkpool or threadpool.
  , scheduling :: Scheduling

    -- | sequence number of most recent copy of the task.
  , newestReplica :: Int

    -- | book keeping for most recent task copy.
  , location :: CurrentLocation
  }

-- | Book keeping of a task. A task is either
--   known to be on a node, or in transition between
--   two nodes over the wire.
data CurrentLocation =
        OnNode NodeId
      | InTransition
        { movingFrom :: NodeId
        , movingTo   :: NodeId }

-- | The task was originally created as a spark
--   or as an eagerly scheduled thread
data Scheduling = Sparked | Pushed deriving (Eq)

instance ToClosure (SupervisedSpark m) where locToClosure = $(here)
