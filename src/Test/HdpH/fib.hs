-- Fibonacci numbers in HdpH
--
-- Author: Patrick Maier
-----------------------------------------------------------------------------

{-# LANGUAGE TemplateHaskell #-}  -- req'd for mkClosure, etc

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (when)
import Data.Functor ((<$>))
import Data.List (elemIndex, stripPrefix)
import Data.Maybe (fromJust)
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf,parseOpts,
        Par, runParIO,
        allNodes, force, fork, spark, spawn, supervisedSpawn, new, get, put, glob, rput,
        GIVar, NodeId,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, static_, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies 
       (parDivideAndConquer, pushDivideAndConquer)
import qualified Control.Parallel.HdpH.FTStrategies as FT (parDivideAndConquer,pushDivideAndConquer)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)
import qualified Control.Parallel.HdpH.FTStrategies as FTStrategies (declareStatic)


-----------------------------------------------------------------------------
-- 'Static' declaration

-- instance ToClosure Int where locToClosure = $(here)
instance ToClosure Integer where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     FTStrategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure Integer),
     declare $(static 'dist_fib_abs),
     declare $(static 'dist_spawn_fib_abs),
     declare $(static 'dist_supervised_spawn_fib_abs),
     declare $(static 'dnc_trivial_abs),
     declare $(static_ 'dnc_decompose),
     declare $(static_ 'dnc_combine),
     declare $(static_ 'dnc_f)]


-----------------------------------------------------------------------------
-- sequential Fibonacci

fib :: Int -> Integer
fib n | n <= 1    = 1
      | otherwise = fib (n-1) + fib (n-2)


-----------------------------------------------------------------------------
-- parallel Fibonacci; shared memory

par_fib :: Int -> Int -> Par Integer
par_fib seqThreshold n
  | n <= k    = force $ fib n
  | otherwise = do v <- new
                   let job = par_fib seqThreshold (n - 1) >>=
                             force >>=
                             put v
                   fork job
                   y <- par_fib seqThreshold (n - 2)
                   x <- get v
                   force $ x + y
  where k = max 1 seqThreshold


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory using spawn

dist_spawn_fib :: Int -> Int -> Int -> Par Integer
dist_spawn_fib seqThreshold parThreshold n
  | n <= k    = force $ fib n
  | n <= l    = par_fib seqThreshold n
  | otherwise = do
      v <- spawn $(mkClosure [| dist_spawn_fib_abs (seqThreshold, parThreshold, n) |])
      y <- dist_spawn_fib seqThreshold parThreshold (n - 2)
      clo_x <- get v
      force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_spawn_fib_abs :: (Int, Int, Int) -> Par (Closure Integer)
dist_spawn_fib_abs (seqThreshold, parThreshold, n) =
  dist_spawn_fib seqThreshold parThreshold (n - 1) >>= return . toClosure

dist_supervised_spawn_fib :: Int -> Int -> Int -> Par Integer
dist_supervised_spawn_fib seqThreshold parThreshold n
  | n <= k    = force $ fib n
  | n <= l    = par_fib seqThreshold n
  | otherwise = do
      v <- supervisedSpawn $(mkClosure [| dist_supervised_spawn_fib_abs (seqThreshold, parThreshold, n) |])
      y <- dist_supervised_spawn_fib seqThreshold parThreshold (n - 2)
      clo_x <- get v
      force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_supervised_spawn_fib_abs :: (Int, Int, Int) -> Par (Closure Integer)
dist_supervised_spawn_fib_abs (seqThreshold, parThreshold, n) =
  dist_supervised_spawn_fib seqThreshold parThreshold (n - 1) >>= return . toClosure

-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory

dist_fib :: Int -> Int -> Int -> Par Integer
dist_fib seqThreshold parThreshold n
  | n <= k    = force $ fib n
  | n <= l    = par_fib seqThreshold n
  | otherwise = do
      v <- new
      gv <- glob v
      spark $(mkClosure [| dist_fib_abs (seqThreshold, parThreshold, n, gv) |])
      y <- dist_fib seqThreshold parThreshold (n - 2)
      clo_x <- get v
      force $ unClosure clo_x + y
  where k = max 1 seqThreshold
        l = parThreshold

dist_fib_abs :: (Int, Int, Int, GIVar (Closure Integer)) -> Par ()
dist_fib_abs (seqThreshold, parThreshold, n, gv) =
  dist_fib seqThreshold parThreshold (n - 1) >>=
  force >>=
  rput gv . toClosure


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory; using sparking d-n-c skeleton

spark_skel_fib :: Int -> Int -> Par Integer
spark_skel_fib seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = parDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

ft_spark_skel_fib :: Int -> Int -> Par Integer
ft_spark_skel_fib seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = FT.parDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

dnc_trivial_abs :: (Int) -> (Closure Int -> Bool)
dnc_trivial_abs (seqThreshold) =
  \ clo_n -> unClosure clo_n <= max 1 seqThreshold

dnc_decompose =
  \ clo_n -> let n = unClosure clo_n in [toClosure (n-1), toClosure (n-2)]

dnc_combine =
  \ _ clos -> toClosure $ sum $ map unClosure clos

dnc_f =
  \ clo_n -> toClosure <$> (force $ fib $ unClosure clo_n)


-----------------------------------------------------------------------------
-- parallel Fibonacci; distributed memory; using pushing d-n-c skeleton

push_skel_fib :: [NodeId] -> Int -> Int -> Par Integer
push_skel_fib nodes seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = pushDivideAndConquer
             nodes
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

ft_push_skel_fib :: Int -> Int -> Par Integer
ft_push_skel_fib seqThreshold n = unClosure <$> skel (toClosure n)
  where 
    skel = FT.pushDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (seqThreshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])


-----------------------------------------------------------------------------
-- initialisation, argument processing and 'main'

-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-- initialize random number generator
initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    setStdGen (mkStdGen seed)


-- parse (optional) arguments in this order: 
-- * version to run
-- * argument to Fibonacci function
-- * threshold below which to execute sequentially
-- * threshold below which to use shared-memory parallelism
parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defN, defSeqThreshold, defParThreshold)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defN,    defSeqThreshold, defParThreshold)
      go v [s1]         = (v, read s1, defSeqThreshold, defParThreshold)
      go v [s1,s2]      = (v, read s1, read s2,         read s2)
      go v (s1:s2:s3:_) = (v, read s1, read s2,         read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

-- defaults for optional arguments
defVers         =  2 :: Int  -- version
defN            = 40 :: Int  -- Fibonacci argument
defParThreshold = 30 :: Int  -- shared-memory threshold
defSeqThreshold = 30 :: Int  -- sequential threshold


main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, seed, args) = parseOpts opts_args
  let (version, n, seqThreshold, parThreshold) = parseArgs args
  initrand seed
  case version of
      0 -> do (x, t) <- timeIO $ evaluate
                          (fib n)
              putStrLn $
                "{v0} fib " ++ show n ++ " = " ++ show x ++
                " {runtime=" ++ show t ++ "}"
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (par_fib seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v1, " ++ 
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_fib seqThreshold parThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v2, " ++
                             "seqThreshold=" ++ show seqThreshold ++ ", " ++
                             "parThreshold=" ++ show parThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (spark_skel_fib seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v3-parDnC, " ++
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      4 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (allNodes >>= \ nodes ->
                                push_skel_fib nodes seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v4-pushDnC, " ++
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      5 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (ft_spark_skel_fib seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v5-parDnCFT, " ++
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      6 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (ft_push_skel_fib seqThreshold n)
              case output of
                Just x  -> putStrLn $
                             "{v6-pushDnCFT, " ++
                             "seqThreshold=" ++ show seqThreshold ++ "} " ++
                             "fib " ++ show n ++ " = " ++ show x ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      _ -> return ()
