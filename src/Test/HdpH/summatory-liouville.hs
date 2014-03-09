-- A fault tolerant HdpH implementation of the summatory liouville function
--
-- Author: Rob Stewart R.Stewart@hw.ac.uk
-- Created: 15th December 2011
--
-- Results comply with the published values of L(x) in paper:
-- "Sign Changes In Sums Of The Liouville Function"
-- Peter Borwein, Ron Ferguson, Michael J. Mossinghoff
-- Mathematics of Computation, 2008

{-# LANGUAGE FlexibleInstances #-} -- for DecodeStatic (Integer,Integer)
{-# LANGUAGE TemplateHaskell #-}

module Main where

import Prelude
import Data.Functor ((<$>))
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import Control.Arrow
import qualified Control.Monad.Par as MonadPar
import qualified Control.Monad.Par.Combinator as MonadPar.C
import Control.Parallel.HdpH 
       (RTSConf(..), defaultRTSConf,chaosMonkeyUnitTest,
        Par, runParIO,allNodes,parseOpts,
        spawn, supervisedSpawn, get,
        IVar,force,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, static_, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies 
       (StaticForceCC,staticForceCC,ForceCC(locForceCC),parMapNF,pushMapNF,parMapSlicedNF)
import qualified Control.Parallel.HdpH.FTStrategies as FT
       (StaticForceCC,staticForceCC,ForceCC(locForceCC),parMapNF,pushMapNF,parMapSlicedNF)
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)
import qualified Control.Parallel.HdpH.FTStrategies as FTStrategies (declareStatic)

import Control.Exception (evaluate)
import Control.Monad (when, (<=<))
import Data.List (stripPrefix,transpose)
import Data.Monoid (mconcat)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Random (mkStdGen, setStdGen)

-----------------------
-- Static declaration

instance ToClosure Integer where locToClosure = $(here)
instance ToClosure (Integer,Integer) where locToClosure = $(here)
instance ForceCC Integer where locForceCC = $(here)
instance FT.ForceCC Integer where locForceCC = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     Strategies.declareStatic,   -- from imported modules
     FTStrategies.declareStatic, -- from imported modules
     declare (staticToClosure :: StaticToClosure Integer),
     declare (staticToClosure :: StaticToClosure (Integer,Integer)),
     declare (staticForceCC :: StaticForceCC Integer),
     declare (FT.staticForceCC :: FT.StaticForceCC Integer),
     declare $(static 'spawn_sum_liouville_abs),
     declare $(static_ 'sumLEvalChunk)]

------------
-- sequential shared-memory implementation

summatoryLiouville :: Integer -> Integer
summatoryLiouville x = sumLEvalChunk (1,x)

sumLEvalChunk :: (Integer,Integer) -> Integer
sumLEvalChunk (lower,upper) =
     let chunkSize = 1000
         smp_chunks = chunk chunkSize [lower,lower+1..upper] :: [[Integer]]
         tuples      = map (head Control.Arrow.&&& last) smp_chunks
     in sum $ map (\(lower,upper) -> sumLEvalChunk' lower upper 0) tuples

sumLEvalChunk' :: Integer -> Integer -> Integer -> Integer
sumLEvalChunk' lower upper total
  | lower > upper = total
  | otherwise = let s = toInteger $ liouville lower
                in sumLEvalChunk' (lower+1) upper (total+s)

liouville :: Integer -> Int
liouville n
  | n == 1 = 1
  | length (primeFactors n) `mod` 2 == 0 = 1
  | otherwise = -1


-- Haskell version taken from:
--   http://www.haskell.org/haskellwiki/99_questions/Solutions/35
primeFactors :: Integer -> [Integer]
primeFactors n = factor primes n
  where 
    factor ps@(p:pt) n | p*p > n      = [n]               
                       | rem n p == 0 = p : factor ps (quot n p) 
                       | otherwise    =     factor pt n
    primes = primesTME

    -- tree-merging Eratosthenes sieve
    -- producing infinite list of all prime numbers
    primesTME = 2 : gaps 3 (join [[p*p,p*p+2*p..] | p <- primes'])
    primes' = 3 : gaps 5 (join [[p*p,p*p+2*p..] | p <- primes'])
    join  ((x:xs):t)        = x : union xs (join (pairs t))
    pairs ((x:xs):ys:t)     = (x : union xs ys) : pairs t
    gaps k xs@(x:t) | k==x  = gaps (k+2) t 
                    | otherwise  = k : gaps (k+2) xs
    -- duplicates-removing union of two ordered increasing lists
    union (x:xs) (y:ys) = case compare x y of 
           LT -> x : union  xs  (y:ys)
           EQ -> x : union  xs     ys 
           GT -> y : union (x:xs)  ys

--------------------------------------
-- parallel Summatory Liouville with monad-par

monadpar_farmParSumLiouville :: Integer -> Int -> MonadPar.Par Integer
monadpar_farmParSumLiouville x chunkSize = do
  let chunked = chunkedList x chunkSize
  sum <$> MonadPar.C.parMap sumLEvalChunk chunked

---------------
-- function closure for spawn family

spawn_sum_liouville :: (Integer,Integer) -> Par (IVar (Closure Integer))
spawn_sum_liouville xs = spawn $(mkClosure [| spawn_sum_liouville_abs (xs) |])

ft_spawn_sum_liouville :: (Integer,Integer) -> Par (IVar (Closure Integer))
ft_spawn_sum_liouville xs = supervisedSpawn $(mkClosure [| spawn_sum_liouville_abs (xs) |])


spawn_sum_liouville_abs :: (Integer, Integer) -> Par (Closure Integer)
spawn_sum_liouville_abs (lower,upper) = 
  force (sumLEvalChunk (lower,upper)) >>= return . toClosure

get_and_unClosure :: IVar (Closure a) -> Par a
get_and_unClosure = return . unClosure <=< get

---------------
-- Using  HdpH primitive 'spawn'

spawnSumLiouville :: Integer -> Int -> Par Integer
spawnSumLiouville x chunkSize = do
  let chunked = chunkedList x chunkSize
  sum <$> (mapM get_and_unClosure =<< (mapM spawn_sum_liouville chunked))

--------------
-- Using parallel map HdpH skeleton

farmParSumLiouville :: Integer -> Int -> Par Integer
farmParSumLiouville x chunkSize = do
  let chunked = chunkedList x chunkSize
  sum <$> parMapNF
           $(mkClosure [|sumLEvalChunk|])
           chunked

farmParSumLiouvilleSliced :: Integer -> Int -> Par Integer
farmParSumLiouvilleSliced x chunkSize = do
  let chunked = chunkedList x chunkSize
  sum <$> parMapSlicedNF chunkSize
           $(mkClosure [|sumLEvalChunk|])
           chunked

farmPushSumLiouville :: Integer -> Int -> Par Integer
farmPushSumLiouville x chunkSize = do
  let chunked = chunkedList x chunkSize
  ns <- allNodes
  sum <$> pushMapNF
           ns
           $(mkClosure [|sumLEvalChunk|])
           chunked

farmPushSumLiouvilleSliced :: Integer -> Int -> Par Integer
farmPushSumLiouvilleSliced x chunkSize = do
  let chunked = concat $ transpose $ chunk chunkSize $ chunkedList x chunkSize
  ns <- allNodes
  sum <$> pushMapNF
           ns
           $(mkClosure [|sumLEvalChunk|])
           chunked

---------------
-- Fault tolerance using  HdpH-RS primitives and FT skeletons

ft_spawnSumLiouville :: Integer -> Int -> Par Integer
ft_spawnSumLiouville x chunkSize = do
  let chunked = chunkedList x chunkSize
  sum <$> (mapM get_and_unClosure =<< mapM ft_spawn_sum_liouville chunked)

ft_farmParSumLiouvilleSliced :: Integer -> Int -> Par Integer
ft_farmParSumLiouvilleSliced x chunkSize = do
  let chunked = chunkedList x chunkSize
  sum <$> FT.parMapSlicedNF chunkSize
           $(mkClosure [|sumLEvalChunk|])
           chunked

ft_farmPushSumLiouvilleSliced :: Integer -> Int -> Par Integer
ft_farmPushSumLiouvilleSliced x chunkSize = do
  let chunked = concat $ transpose $ chunk chunkSize $ chunkedList x chunkSize
  sum <$> FT.pushMapNF
           $(mkClosure [|sumLEvalChunk|])
           chunked


--------
-- Utility functions

chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk n xs = ys : chunk n zs where (ys,zs) = splitAt n xs

chunkedList :: Integer -> Int -> [(Integer,Integer)]
chunkedList x chunkSize = zip lowers uppers
    where
  lowers       = [1, toInteger (chunkSize + 1) .. x]
  uppers       = [toInteger chunkSize, toInteger (chunkSize*2) .. x]++[x]

-- time an IO action
timeIO :: IO a -> IO (a, NominalDiffTime)
timeIO action = do t0 <- getCurrentTime
                   x <- action
                   t1 <- getCurrentTime
                   return (x, diffUTCTime t1 t0)


-- initialize random number generator
initrand :: Int -> IO ()
initrand seed =
  when (seed /= 0) $
    setStdGen (mkStdGen seed)

-- parse (optional) arguments in this order: 
-- * version to run
-- * input to summatory liouville function
-- * size of chunks (evaluated sequentially)
parseArgs :: [String] -> (Int, Integer, Int,Integer)
parseArgs []     = (defVers, defX, defChunk,defExpected)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Integer, Int, Integer)
      go v []           = (v, defX, defChunk,defExpected)
      go v [s1]         = (v, read s1,  defChunk,defExpected)
      go v [s1,s2]      = (v, read s1, read s2,defExpected)
      go v (s1:s2:s3:_)      = (v, read s1, read s2,read s3)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

defVers, defChunk :: Int
defVers  = 0 
defChunk = 10000

defX,defExpected :: Integer
defX = 100000
defExpected = 0

main :: IO ()
main = do
   hSetBuffering stdout LineBuffering
   hSetBuffering stderr LineBuffering
   register declareStatic
   opts_args <- getArgs
   let (conf, seed, args) = parseOpts opts_args
       (version, x, chunkSize,expected) = parseArgs args
   initrand seed
   case version of
     0 -> do (y, t) <- timeIO $ evaluate
               (summatoryLiouville x)
             putStrLn $
               "{v0} sum $ liouville " ++ show x ++ " = "
               ++ show y ++ " {runtime=" ++ show t ++ "}"
     1 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
                (spawnSumLiouville x chunkSize)
             case output of
               Just y -> putStrLn $
                 "{v1-spawn}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
               Nothing -> return ()
     2 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
               (farmParSumLiouville x chunkSize)
             case output of
               Just y -> putStrLn $
                 "{v2-parMap}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
               Nothing -> return ()
     3 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
               (farmPushSumLiouville x chunkSize)
             case output of
               Just y -> putStrLn $
                 "{v3-pushMap}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
               Nothing -> return ()
     4 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
               (ft_spawnSumLiouville x chunkSize)
             case output of
               Just y -> putStrLn $
                 "{v4-supervisedSpawn}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
               Nothing -> return ()
     5 -> do if (chaosMonkey conf)
              then chaosMonkeyUnitTest conf "summatoryLiouville-parMapFT" expected (ft_farmParSumLiouvilleSliced x chunkSize)
              else do
               (output, t) <- timeIO $ evaluate =<< runParIO conf
                   (ft_farmParSumLiouvilleSliced x chunkSize)
               case output of
                 Just y -> putStrLn $
                   "{v5-parMapFT}, chunksize=" ++ show chunkSize ++ "} " ++
                   "sum $ liouville " ++ show x ++ " = " ++
                   show y ++ " {runtime=" ++ show t ++ "}"
                 Nothing -> return ()
     6 -> do if (chaosMonkey conf)
              then chaosMonkeyUnitTest conf "summatoryLiouville-parMapFT" expected (ft_farmPushSumLiouvilleSliced x chunkSize)
              else do
               (output, t) <- timeIO $ evaluate =<< runParIO conf
                  (ft_farmPushSumLiouvilleSliced x chunkSize)
               case output of
                 Just y -> putStrLn $
                   "{v6-pushMapFT}, chunksize=" ++ show chunkSize ++ "} " ++
                   "sum $ liouville " ++ show x ++ " = " ++
                   show y ++ " {runtime=" ++ show t ++ "}"
                 Nothing -> return ()
     7 -> do (output, t) <- timeIO $ evaluate =<< return (MonadPar.runPar
               (monadpar_farmParSumLiouville x chunkSize))
             case output of
               y -> putStrLn $
                 "{v7-monadpar}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
     8 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
               (farmParSumLiouvilleSliced x chunkSize)
             case output of
               Just y -> putStrLn $
                 "{v8-parMapSliced}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
               Nothing -> return ()
     9 -> do (output, t) <- timeIO $ evaluate =<< runParIO conf
               (farmPushSumLiouvilleSliced x chunkSize)
             case output of
               Just y -> putStrLn $
                 "{v9-pushMapSliced}, chunksize=" ++ show chunkSize ++ "} " ++
                 "sum $ liouville " ++ show x ++ " = " ++
                 show y ++ " {runtime=" ++ show t ++ "}"
               Nothing -> return ()
     _ -> error "unrecognised version"

