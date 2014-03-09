-- Hello World in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}

module Main where

import Prelude
import Data.List (stripPrefix)
import Data.Monoid (mconcat)
import System.Environment (getArgs)
-- import System.Exit
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Mem (performGC)
import System.Posix.Signals
import System.Random (randomIO)
import Control.Applicative ((<$>))
import Control.Concurrent
import Control.Monad (when)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf,parseOpts,
        Par, runParIO_,
        myNode, allNodes, io, pushTo, new, get, glob, rput,
        spawn,spawnAt,
        supervisedSpawn, supervisedSpawnAt, force,
        NodeId, IVar, GIVar,
        Closure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)


-----------------------------------------------------------------------------
-- Static declaration

instance ToClosure () where locToClosure = $(here)
instance ToClosure Bool where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic = mconcat [HdpH.declareStatic,
                         declare (staticToClosure :: StaticToClosure ()),
                         declare (staticToClosure :: StaticToClosure Bool),
                         declare $(static 'delayed_hello_abs)]


-----------------------------------------------------------------------------
-- Hello World code

-- | Each spawned task will count to 5 and return True
-- One node will die after 2 seconds, and the task will
-- be rescheduled in the threadpool of the master node.
test_spawn_at :: Par ()
test_spawn_at = do
  master <- myNode
  io $ putStrLn $ "Master " ++ show master
  all <- allNodes
  let world = filter (/=master) all
  poisonedNode <- io $ randomElement world
  let xs = zip world (map (==poisonedNode) world)
  vs <- mapM push_hello xs
  mapM_ get vs
    where
      push_hello :: (NodeId,Bool) -> Par (IVar (Closure ()))
      push_hello (node,poisoned) = supervisedSpawnAt $(mkClosure [| delayed_hello_abs (node,poisoned) |]) node

delayed_hello_abs :: (NodeId,Bool) -> Par (Closure ())
delayed_hello_abs (firstNodeAllocatedThis,willDie) = do
  me <- myNode
  io $ when (willDie && me == firstNodeAllocatedThis) $ do
      putStrLn ((show me) ++ ": I am going to die")
      raiseSignal killProcess
--          a exitFailure after 2 seconds if willDie
  here <- myNode
  io $ threadDelay 3000000
  io $ putStrLn $ "Hello from " ++ show here
  force $ toClosure ()

randomElement :: [a] -> IO a
randomElement xs = randomIO >>= \ix -> return (xs !! (ix `mod` length xs))

-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, _args) = parseOpts opts_args
  runParIO_ conf test_spawn_at
