-- Global references; types
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------
{-# LANGUAGE DeriveGeneric
           , DeriveDataTypeable #-}

module Control.Parallel.HdpH.Internal.Type.GRef
  ( -- * global references
    GRef(..),

    -- * registry for global references
--    GRefReg(..),

    TaskRef(..),

    taskHandle
  ) where

import Prelude

import Control.Parallel.HdpH.Internal.Location (NodeId)
-- import Control.Parallel.HdpH.Internal.Misc (AnyType)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Control.DeepSeq (NFData)

-----------------------------------------------------------------------------
-- global references

-- Global references, comprising a locally unique slot on the hosting node
-- and the hosting node's ID. Note that the slot, represented by a positive
-- integer, must be unique over the life time of the hosting node (ie. it
-- can not be reused). Note also that the type constructor 'GRef' takes
-- a phantom type parameter, tracking the type of the referenced object.
data GRef a = GRef { slot :: !Int,
                     at   :: !NodeId }

data TaskRef = TaskRef { slotT :: !Int,
                         atT   :: !NodeId }
                       deriving (Generic,Typeable,Eq,Show)
instance Binary TaskRef
instance NFData TaskRef

taskHandle :: GRef a -> TaskRef
taskHandle g = TaskRef { slotT = slot g , atT = at g }

