{-# LANGUAGE DeriveDataTypeable #-}
module Control.Parallel.HdpH.Internal.Type.Msg where

import Control.Parallel.HdpH.Internal.Type.Par -- (Task)
import Control.Parallel.HdpH.Internal.Type.GRef (TaskRef)
import Control.Parallel.HdpH.Internal.Location (NodeId)
import Control.DeepSeq (NFData, rnf)
-- import Data.Serialize (Serialize)
import Data.Binary (Binary)
import qualified Data.Binary (put, get)
import Data.Word (Word8)

-----------------------------------------------------------------------------
-- HdpH messages (peer to peer)

-- 6 different types of messages dealing with fishing and pushing sparks;
-- the parameter 's' abstracts the type of sparks
data Msg m = PUSH        -- eagerly pushing work
               (Task m)   -- task
           | FISH        -- looking for work
               !NodeId     -- fishing node
           | SCHEDULE    -- reply to FISH sender (when there is work)
               (Task m)   -- spark
               !NodeId     -- sender
           | NOWORK      -- reply to FISH sender (when there is no work)
           | REQ
               TaskRef   -- The globalized spark pointer
               !Int      -- sequence number of task
               !NodeId   -- the node it would move from
               !NodeId   -- the node it has been scheduled to
           | AUTH
               TaskRef   -- Spark to SCHEDULE onwards
               !NodeId   -- fishing node to send SCHEDULE to
           | DENIED
               !NodeId   -- fishing node to return NOWORK to
           | ACK         -- notify the supervising node that a spark has been scheduled here
               TaskRef -- The globalized spark pointer
               !Int      -- sequence number of task
               !NodeId   -- the node receiving the spark, just a sanity check
           | DEADNODE    -- a node has died (propagated from the transport layer)
               !NodeId   -- which node has died
           | OBSOLETE    -- absolete task copy (old sequence number)
               !NodeId   -- fishing node waiting for guarded spark (to receive NOWORK)
           | HEARTBEAT   -- keep-alive heartbeat message

-- Show instance (mainly for debugging)
instance Show (Msg m) where
  showsPrec _ (PUSH _spark)             = showString "PUSH(_)"
  showsPrec _ (FISH fisher)             = showString "FISH(" . shows fisher .
                                          showString ")"
  showsPrec _ (SCHEDULE _spark sender)  = showString "SCHEDULE(_," .
                                          shows sender . showString ")"
  showsPrec _ (NOWORK)                  = showString "NOWORK"
  showsPrec _ (REQ _sparkHandle thisSeq from to)     = showString "REQ(_," .
                                          shows thisSeq . showString "," .
                                          shows from . showString "," .
                                          shows to . showString ")"

  showsPrec _ (AUTH _sparkHandle to)    = showString "AUTH(_," .
                                          shows to . showString ")"
  showsPrec _ (DENIED to)                  = showString "DENIED(" .
                                             shows to . showString ")"
  showsPrec _ (ACK _gpsark _seqN newNode)  = showString "ACK(_," .
                                          shows newNode .
                                          showString ")"
  showsPrec _ (DEADNODE deadNode)  = showString "DEADNODE(_," .
                                          shows deadNode . showString ")"
  showsPrec _ (OBSOLETE fisher)              = showString "OBSOLETE" .
                                          shows fisher . showString ")"
  showsPrec _ (HEARTBEAT)                  = showString "HEARTBEAT"

instance NFData (Msg m) where
  rnf (PUSH spark)              = rnf spark
  rnf (FISH fisher)             = rnf fisher
  rnf (SCHEDULE spark sender)   = rnf spark `seq` rnf sender
  rnf (NOWORK)                  = ()
  rnf (REQ sparkHandle seqN from to)   = rnf sparkHandle `seq` rnf seqN `seq` rnf from `seq` rnf to
  rnf (AUTH sparkHandle to)                  = rnf sparkHandle `seq` rnf to
  rnf (DENIED to)                  = rnf to
  rnf (ACK sparkHandle seqN newNode)   = rnf sparkHandle `seq` rnf seqN `seq` rnf newNode
  rnf (DEADNODE deadNode)           = rnf deadNode
  rnf (OBSOLETE fisher)           = rnf fisher
  rnf (HEARTBEAT)                  = ()

instance Binary (Msg m) where
  put (PUSH spark)              = Data.Binary.put (0 :: Word8) >>
                                  Data.Binary.put spark
  put (FISH fisher)             = Data.Binary.put (1 :: Word8) >>
                                  Data.Binary.put fisher
  put (SCHEDULE spark sender)   = Data.Binary.put (2 :: Word8) >>
                                  Data.Binary.put spark >>
                                  Data.Binary.put sender
  put (NOWORK)                  = Data.Binary.put (3 :: Word8)
  put (REQ sparkHandle seqN from to) = Data.Binary.put (4 :: Word8) >>
                                  Data.Binary.put sparkHandle >>
                                  Data.Binary.put seqN >>
                                  Data.Binary.put from >>
                                  Data.Binary.put to
  put (AUTH sparkHandle to)   = Data.Binary.put (5 :: Word8) >>
                                  Data.Binary.put sparkHandle >>
                                  Data.Binary.put to
  put (DENIED fishingNode)    = Data.Binary.put (6 :: Word8) >>
                                Data.Binary.put fishingNode
  put (ACK sparkHandle seqN fishingNode) = Data.Binary.put (7 :: Word8) >>
                                  Data.Binary.put sparkHandle >>
                                  Data.Binary.put seqN >>
                                  Data.Binary.put fishingNode
  put (DEADNODE deadNode)       = Data.Binary.put (8 :: Word8) >>
                                  Data.Binary.put deadNode
  put (OBSOLETE fisher)         = Data.Binary.put (9 :: Word8) >>
                              Data.Binary.put fisher
  put (HEARTBEAT)               = Data.Binary.put (10 :: Word8)

  get = do tag <- Data.Binary.get
           case tag :: Word8 of
             0 -> do spark <- Data.Binary.get
                     return $ PUSH spark
             1 -> do fisher <- Data.Binary.get
                     return $ FISH fisher
             2 -> do spark <- Data.Binary.get
                     sender <- Data.Binary.get
                     return $ SCHEDULE spark sender
             3 -> do return $ NOWORK
             4 -> do sparkHandle <- Data.Binary.get
                     seqN <- Data.Binary.get
                     from <- Data.Binary.get
                     to <- Data.Binary.get
                     return $ REQ sparkHandle seqN from to
             5 -> do sparkHandle <- Data.Binary.get
                     to <- Data.Binary.get
                     return $ AUTH sparkHandle to
             6 -> do fishingNode <- Data.Binary.get
                     return $ DENIED fishingNode
             7 -> do sparkHandle <- Data.Binary.get
                     seqN <- Data.Binary.get
                     fishingNode <- Data.Binary.get
                     return $ ACK sparkHandle seqN fishingNode
             8 -> do deadNode <- Data.Binary.get
                     return $ DEADNODE deadNode
             9 -> do fisher <- Data.Binary.get
                     return $ OBSOLETE fisher
             10 -> do return $ HEARTBEAT
