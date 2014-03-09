{-# LANGUAGE TemplateHaskell #-}      -- req'd for 'mkClosure', et
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}


module Control.Parallel.HdpH.Internal.Type.ToClosureInstances where

import Data.Binary (Binary)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Data.Monoid (mconcat)

import Control.Parallel.HdpH
    (here,ToClosure(locToClosure),
        StaticToClosure, staticToClosure,
        StaticDecl, declare)

-- these are needed by Strategies and FTStrategies modules
-- but cannot be declared in both, and then imported by a user application.

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure Bool),
     declare (staticToClosure :: StaticToClosure InclusiveRange)]

instance ToClosure Int where locToClosure = $(here)
instance ToClosure Bool where locToClosure = $(here)
instance ToClosure InclusiveRange where locToClosure = $(here)

data InclusiveRange = InclusiveRange Int Int
                      deriving (Typeable,Generic)
instance Binary InclusiveRange

