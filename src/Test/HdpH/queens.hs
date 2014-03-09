-- Queens in HdpH
--
-- Author: Rob Stewart
-- Adapted from monad-par implementation:
--    https://github.com/simonmar/monad-par/blob/master/examples/src/queens.hs
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc
{-# LANGUAGE BangPatterns #-}

module Main where

import Prelude
import Control.Exception (evaluate)
import Control.Monad (when)
import qualified Control.Monad.Par as MonadPar
import qualified Control.Monad.Par.Combinator as MonadPar.C
import Data.List (stripPrefix)
import Data.Functor ((<$>))
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf,io,
        Par, runParIO,parseOpts,chaosMonkeyUnitTest,
        force, spawn, supervisedSpawn, get,allNodes,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, static_, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies
          (forkDivideAndConquer,parDivideAndConquer,pushDivideAndConquer)
import qualified Control.Parallel.HdpH.FTStrategies as FT (parDivideAndConquer,pushDivideAndConquer)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)
import qualified Control.Parallel.HdpH.FTStrategies as FTStrategies (declareStatic)

instance ToClosure [[Int]] where locToClosure = $(here)
instance ToClosure (Int, Int, [Int]) where locToClosure = $(here)
instance ToClosure Float where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,           -- declare Static deserialisers
     Strategies.declareStatic,     -- from imported modules
     FTStrategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure Int),
     declare (staticToClosure :: StaticToClosure [[Int]]),
     declare (staticToClosure :: StaticToClosure (Int,Int,[Int])),
     declare (staticToClosure :: StaticToClosure Float),
     declare $(static 'dist_queens_abs),
     declare $(static 'ft_dist_queens_abs),
     declare $(static 'dnc_trivial_abs),
     declare $(static_ 'dnc_decompose),
     declare $(static_ 'dnc_combine),
     declare $(static_ 'dnc_f)]

-------------------------------------------------------

-- parallel Queens using distributed memory using monad-par
--  From: https://github.com/simonmar/monad-par/blob/master/examples/src/queens.hs

monadpar_queens :: Int -> Int -> MonadPar.Par [[Int]]
monadpar_queens nq threshold = step 0 []
  where
    step :: Int -> [Int] -> MonadPar.Par [[Int]]
    step !n b
       | n >= threshold = return (iterate (gen nq) [b] !! (nq - n))
       | otherwise = do
          rs <- MonadPar.C.parMapM (step (n+1)) (gen nq [b])
          return (concat rs)

-------------------------------------------------------
-- parallel Queens using distributed memory using spawn

dist_queens = dist_queens' 0 []

dist_queens' :: Int -> [Int] -> Int -> Int -> Par [[Int]]
dist_queens' !n b nq threshold
  | n >= threshold = force $ iterate (gen nq) [b] !! (nq - n)
  | otherwise = do
      let n' = n+1
      vs <- mapM (\b' -> spawn $(mkClosure [| dist_queens_abs (n',b',nq,threshold) |]) ) (gen nq [b])
      rs <- mapM (get) vs
      force $ concatMap unClosure rs

dist_queens_abs :: (Int,[Int],Int,Int) -> Par (Closure [[Int]])
dist_queens_abs (n,b,nq,threshold) =
  dist_queens' n b nq threshold >>= return . toClosure

-------------------------------------------------------
-- Fault Tolerant parallel Queens using distributed memory using spawn

ft_dist_queens = ft_dist_queens' 0 []

ft_dist_queens' :: Int -> [Int] -> Int -> Int -> Par [[Int]]
ft_dist_queens' !n b nq threshold
  | n >= threshold = force $ iterate (gen nq) [b] !! (nq - n)
  | otherwise = do
      let n' = n+1
      vs <- mapM (\b' -> supervisedSpawn $(mkClosure [| dist_queens_abs (n',b',nq,threshold) |]) ) (gen nq [b])
      rs <- mapM (get) vs
      force $ concatMap unClosure rs

ft_dist_queens_abs :: (Int,[Int],Int,Int) -> Par (Closure [[Int]])
ft_dist_queens_abs (n,b,nq,threshold) =
  ft_dist_queens' n b nq threshold >>= return . toClosure

-------------------------------------------------------
-- parallel (with fork) Queens using distributed memory using fork DnC skeleton

dist_skel_fork_queens :: Int -> Int -> Par [[Int]]
dist_skel_fork_queens nq threshold = skel (0,nq,[])
  where
    skel = forkDivideAndConquer trivial decompose combine f
    trivial (n',_,_) = n' >= threshold
    decompose (n',nq',b') = map (\b'' -> (n'+1,nq',b'')) (gen nq' [b'])
    combine _ a = concat a
    f (n',nq',b') = force (iterate (gen nq') [b'] !! (nq' - n'))


-------------------------------------------------------
-- parallel Queens using DnC skeleton

dist_skel_par_queens :: Int -> Int -> Par [[Int]]
dist_skel_par_queens nq threshold = unClosure <$> skel (toClosure (0,nq,[]))
  where
    skel = parDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (threshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

dist_skel_push_queens :: Int -> Int -> Par [[Int]]
dist_skel_push_queens nq threshold = unClosure <$> skel (toClosure (0,nq,[]))
  where
    skel x_clo = do
            nodes <- allNodes
            pushDivideAndConquer
             nodes
             $(mkClosure [| dnc_trivial_abs (threshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])
             x_clo

ft_dist_skel_par_queens :: Int -> Int -> Par [[Int]]
ft_dist_skel_par_queens nq threshold = unClosure <$> skel (toClosure (0,nq,[]))
  where
    skel = FT.parDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (threshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

ft_dist_skel_push_queens :: Int -> Int -> Par [[Int]]
ft_dist_skel_push_queens nq threshold = unClosure <$> skel (toClosure (0,nq,[]))
  where
    skel = FT.pushDivideAndConquer
             $(mkClosure [| dnc_trivial_abs (threshold) |])
             $(mkClosure [| dnc_decompose |])
             $(mkClosure [| dnc_combine |])
             $(mkClosure [| dnc_f |])

dnc_trivial_abs :: (Int) -> (Closure  (Int,Int,[Int]) -> Bool)
dnc_trivial_abs threshold =
  \ clo_n -> let (!n,_nq,_b) = unClosure clo_n in n >= threshold

dnc_decompose :: Closure (Int, Int, [Int]) -> [Closure (Int, Int, [Int])]
dnc_decompose =
  \ clo_n -> let (n,nq,b) = unClosure clo_n
             in map (\b' -> toClosure (n+1,nq,b')) (gen nq [b])

dnc_combine ::  a -> [Closure [[Int]]] -> Closure [[Int]]
dnc_combine =
  \ _ clos -> toClosure $ concatMap unClosure clos

dnc_f :: Closure (Int, Int, [Int]) -> Par (Closure [[Int]])
dnc_f =
  \ clo_n -> do
       let (n,nq,b) = unClosure clo_n
       toClosure <$> force (iterate (gen nq) [b] !! (nq - n))

------------------------------------------------------
-- pure functions for Queens

safe :: Int -> Int -> [Int] -> Bool
safe _ _ []    = True
safe x d (q:l) = x /= q && x /= q+d && x /= q-d && safe x (d+1) l

gen :: Int -> [[Int]] -> [[Int]]
gen nq bs = [ q:b | b <- bs, q <- [1..nq], safe q 1 b ]

-------------------------------------------------------


parseArgs :: [String] -> (Int, Int, Int, Int)
parseArgs []     = (defVers, defN, defThreshold,defExpected)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int)
      go v []           = (v, defN,    defThreshold,defExpected)
      go v [s1]         = (v, read s1, defThreshold,defExpected)
      go v [s1,s2]      = (v, read s1, read s2, defExpected)
      go v (s1:s2:s3:_) = (v, read s1, read s2, read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

defVers, defN, defThreshold, defExpected :: Int
defVers         = 0  -- version
defN            = 10 -- Queens argument
defThreshold    = 5  -- threshold
defExpected     = 0  -- expected value of Par computation

timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)

initrand :: Int -> IO ()
initrand seed = do
  when (seed /= 0) $ do
    setStdGen (mkStdGen seed)

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, seed, args) = parseOpts opts_args
  let (version, n, threshold,expected) = parseArgs args
  initrand seed
  case version of
      -- Non-Fault Tolerant implementations
      0 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_queens n threshold)
              case output of
                Just x  -> putStrLn $
                             "{v0-dist-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_skel_par_queens n threshold)
              case output of
                Just x  -> putStrLn $
                             "{v1-dist-skel-par-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_skel_push_queens n threshold)
              case output of
                Just x  -> putStrLn $
                             "{v2-dist-skel-push-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()

      -- Fault Tolerant implementations
      3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (ft_dist_queens n threshold)
              case output of
                Just x  -> putStrLn $
                             "{v3-ft-dist-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      4 -> do if (chaosMonkey conf)
               then chaosMonkeyUnitTest conf "sumeuler-parDnCFT" expected (length <$> ft_dist_skel_par_queens n threshold)
               else do
                (output, t) <- timeIO $ evaluate =<< runParIO conf
                                (ft_dist_skel_par_queens n threshold)
                case output of
                  Just x  -> putStrLn $
                             "{v4-ft-dist-skel-par-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                  Nothing -> return ()
      5 -> do if (chaosMonkey conf)
               then chaosMonkeyUnitTest conf "sumeuler-pushDnCFT" expected (length <$> ft_dist_skel_push_queens n threshold)
               else do
                (output, t) <- timeIO $ evaluate =<< runParIO conf
                                 (ft_dist_skel_push_queens n threshold)
                case output of
                 Just x  -> putStrLn $
                             "{v5-ft-dist-skel-push-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                 Nothing -> return ()

      -- special case: shared-memory forking
      8 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                               (dist_skel_fork_queens n threshold)
              case output of
                Just x  -> putStrLn $
                             "{v8-skel-fork-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
                Nothing -> return ()
      -- monad-par
      9 -> do (output, t) <- timeIO $ evaluate =<< return (MonadPar.runPar
                               (monadpar_queens n threshold))
              case output of
                x  -> putStrLn $
                             "{v9-monadpar-queens, " ++
                             "threshold=" ++ show threshold ++ "} " ++
                             "queens " ++ show n ++ " = " ++ show (length x) ++
                             " {runtime=" ++ show t ++ "}"
      _ -> return ()