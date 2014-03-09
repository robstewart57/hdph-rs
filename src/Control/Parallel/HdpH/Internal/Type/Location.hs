-- Locations; types; wrapper module
--
-- Author: Rob Stewart, Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE DeriveDataTypeable #-}  -- for defining exceptions
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Control.Parallel.HdpH.Internal.Type.Location
  ( -- * node IDs (and their constitutent parts)
    NodeId,

    -- * node ID exception
    MyNodeException(..)  -- instances: Exception, Show, Typeable
  ) where

import Prelude
import Control.DeepSeq (NFData)
import Control.Exception (Exception)
import Data.Binary (Binary)
import Data.Typeable (Typeable)
import Network.Transport (EndPointAddress(..))


-----------------------------------------------------------------------------
-- node IDs (should be abstract and hyperstrict outwith this module)
-- HACK: identify node ID with end point

-- | A 'NodeId' identifies a node (that is, an OS process running HdpH).
-- A 'NodeId' should be thought of as an abstract identifier (though it is
-- not currently abstract) which instantiates the classes 'Eq', 'Ord',
-- 'Show', 'NFData' and 'Binary'.
type NodeId = EndPointAddress

deriving instance NFData NodeId
-- deriving instance Binary NodeId


-----------------------------------------------------------------------------
-- exception raised when ID of this node is not set

data MyNodeException = NodeIdUnset
                       deriving (Show, Typeable)

instance Exception MyNodeException
