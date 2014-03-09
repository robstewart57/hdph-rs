-- A Mandelbrot set creation in HdpH
--
-- Author: Rob Stewart
-- Date: 10th July 2013
-- Adapted from monad-par implementation
--   monad-par paper: http://research.microsoft.com/en-us/um/people/simonpj/papers/parallel/monad-par.pdf
--   monad-par implemenation: https://raw.github.com/simonmar/monad-par/master/examples/src/mandel.hs
--
-- Note: Mandelbrot is a case study Section 4 of
--       "Visualizing Parallel Functional Program Runs - Case Studies with the Eden Trace Viewer"
--       by Jost Berthold and Rita Loogen
--       http://www.mathematik.uni-marburg.de/~eden/paper/ParCo07EdenTV.pdf
-----------------------------------------------------------------------------

{-# LANGUAGE FlexibleInstances #-}  -- req'd for some ToClosure instances
{-# LANGUAGE TemplateHaskell #-}    -- req'd for mkClosure, etc
{-# LANGUAGE BangPatterns #-} -- req'd for mandel function
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module Main where

import Prelude
import Control.DeepSeq hiding (force)
import Control.Exception (evaluate)
import Control.Monad (when)
import qualified Control.Monad.Par as MonadPar
import qualified Control.Monad.Par.Combinator as MonadPar.C
import Data.Complex
import Data.Functor ((<$>))
import Data.List (stripPrefix)
import Data.List.Split (splitOn)
import Data.Maybe (isJust,fromJust)
import Data.Monoid (mconcat)
import Data.Binary (Binary)
import Data.Time.Clock (NominalDiffTime, diffUTCTime, getCurrentTime)
import Data.Typeable (Typeable)
import Data.Vector.Binary ()
import qualified Data.Vector.Unboxed as V
import GHC.Generics (Generic)
import System.Environment (getArgs)
import System.IO (stdout, stderr, hSetBuffering, BufferMode(..))
import System.Random (mkStdGen, setStdGen)

import Control.Parallel.HdpH
       (RTSConf(..), defaultRTSConf,parseOpts,
        Par, runParIO,chaosMonkeyUnitTest,
        Closure, unClosure, mkClosure,
        toClosure, ToClosure(locToClosure),
        static, static_, StaticToClosure, staticToClosure,
        StaticDecl, declare, register, here)
import qualified Control.Parallel.HdpH as HdpH (declareStatic)
import Control.Parallel.HdpH.Strategies
       (parMapReduceRangeThresh,pushMapReduceRangeThresh,InclusiveRange(..))
import qualified Control.Parallel.HdpH.FTStrategies as FT
       (parMapReduceRangeThresh,pushMapReduceRangeThresh,InclusiveRange(..))
import qualified Control.Parallel.HdpH.Strategies as Strategies (declareStatic)
import qualified Control.Parallel.HdpH.FTStrategies as FTStrategies (declareStatic)

-----------------------------------------------------------------------------
-- Static declaration

instance ToClosure VecTree where locToClosure = $(here)

declareStatic :: StaticDecl
declareStatic =
  mconcat
    [HdpH.declareStatic,         -- declare Static deserialisers
     FTStrategies.declareStatic,   -- from imported modules
     Strategies.declareStatic,   -- from imported modules
     declare (staticToClosure :: StaticToClosure VecTree),
     declare $(static 'map_f),
     declare $(static_ 'reduce_f),
     declare $(static_ 'mandel)]

----------------------------------------------------------------------------
-- sequential mandel function

mandel :: Int -> Complex Double -> Int
mandel max_depth c = loop 0 0
  where   
   fn = magnitude
   loop i !z
    | i == max_depth = i
    | fn(z) >= 2.0   = i
    | otherwise      = loop (i+1) (z*z + c)

checkSum :: VecTree -> Int
checkSum (Leaf vec) = V.foldl (+) 0 vec
checkSum (MkNode v1 v2) = checkSum v1 + checkSum v2

data VecTree = Leaf (V.Vector Int)
             | MkNode VecTree VecTree
             deriving (Eq,Show,Generic,Typeable)
instance Binary VecTree
instance NFData VecTree

-----------------------------------------------
-- Mandel using monad-par
--  From: https://github.com/simonmar/monad-par/blob/master/examples/src/mandel.hs

monadpar_runMandel :: Int -> Int -> Int -> Int -> MonadPar.Par VecTree
monadpar_runMandel = monadpar_runMandel' (-2) (-2) 2 2

monadpar_runMandel' :: Double -> Double -> Double -> Double -> Int -> Int -> Int -> Int -> MonadPar.Par VecTree
monadpar_runMandel' minX minY maxX maxY winX winY max_depth threshold = do
  MonadPar.C.parMapReduceRangeThresh threshold (MonadPar.C.InclusiveRange 0 (winY-1)) 
     (\y -> 
       do
          let vec = V.generate winX (\x -> mandelStep y x)
          seq (vec V.! 0) $ 
           return (Leaf vec))
     (\ a b -> return$ MkNode a b)
     (Leaf V.empty)
  where
    mandelStep i j = mandel max_depth z
        where z = ((fromIntegral j * r_scale) / fromIntegral winY + minY) :+
                  ((fromIntegral i * c_scale) / fromIntegral winX + minX)
    r_scale  =  maxY - minY  :: Double
    c_scale =   maxX - minX  :: Double


-----------------------------------------------
-- Mandel using mapReduceRangeThresh family

dist_skel_par_mandel :: Int -> Int -> Int -> Int -> Par VecTree
dist_skel_par_mandel = runMandelPar (-2) (-2) 2 2

dist_skel_push_mandel :: Int -> Int -> Int -> Int -> Par VecTree
dist_skel_push_mandel = runMandelPush (-2) (-2) 2 2

ft_dist_skel_par_mandel :: Int -> Int -> Int -> Int -> Par VecTree
ft_dist_skel_par_mandel = runMandelParFT (-2) (-2) 2 2

ft_dist_skel_push_mandel :: Int -> Int -> Int -> Int -> Par VecTree
ft_dist_skel_push_mandel = runMandelPushFT (-2) (-2) 2 2

runMandelPar :: Double -> Double -> Double -> Double -> Int -> Int -> Int -> Int -> Par VecTree
runMandelPar minX minY maxX maxY winX winY maxDepth threshold =
    unClosure <$> skel (toClosure (Leaf V.empty))
  where 
    skel = parMapReduceRangeThresh
            (toClosure threshold)
            (toClosure (InclusiveRange 0 (winY-1)))
            $(mkClosure [| map_f (minX,minY,maxX,maxY,winX,winY,maxDepth) |])
            $(mkClosure [| reduce_f |])

runMandelPush :: Double -> Double -> Double -> Double -> Int -> Int -> Int -> Int -> Par VecTree
runMandelPush minX minY maxX maxY winX winY maxDepth threshold =
    unClosure <$> skel (toClosure (Leaf V.empty))
  where 
    skel = pushMapReduceRangeThresh
            (toClosure threshold)
            (toClosure (InclusiveRange 0 (winY-1)))
            $(mkClosure [| map_f (minX,minY,maxX,maxY,winX,winY,maxDepth) |])
            $(mkClosure [| reduce_f |])

runMandelParFT :: Double -> Double -> Double -> Double -> Int -> Int -> Int -> Int -> Par VecTree
runMandelParFT minX minY maxX maxY winX winY maxDepth threshold =
    unClosure <$> skel (toClosure (Leaf V.empty))
  where 
    skel = FT.parMapReduceRangeThresh
            (toClosure threshold)
            (toClosure (FT.InclusiveRange 0 (winY-1)))
            $(mkClosure [| map_f (minX,minY,maxX,maxY,winX,winY,maxDepth) |])
            $(mkClosure [| reduce_f |])

runMandelPushFT :: Double -> Double -> Double -> Double -> Int -> Int -> Int -> Int -> Par VecTree
runMandelPushFT minX minY maxX maxY winX winY maxDepth threshold =
    unClosure <$> skel (toClosure (Leaf V.empty))
  where 
    skel = FT.pushMapReduceRangeThresh
            (toClosure threshold)
            (toClosure (FT.InclusiveRange 0 (winY-1)))
            $(mkClosure [| map_f (minX,minY,maxX,maxY,winX,winY,maxDepth) |])
            $(mkClosure [| reduce_f |])

------------------------------------------------
-- Map and Reduce function closure implementations

map_f :: (Double,Double,Double,Double,Int,Int,Int)
      -> Closure Int
      -> Par (Closure VecTree)
map_f (minX,minY,maxX,maxY,winX,winY,maxDepth) = \y_clo -> do
    let y = unClosure y_clo
    let vec = V.generate winX (\x -> mandelStep y x)
    seq (vec V.! 0) $ return (toClosure (Leaf vec))
  where
    mandelStep i j = mandel maxDepth (calcZ i j)
    calcZ i j = ((fromIntegral j * r_scale) / fromIntegral winY + minY) :+
           ((fromIntegral i * c_scale) / fromIntegral winX + minX)
    r_scale =  maxY - minY  :: Double
    c_scale =   maxX - minX  :: Double

reduce_f :: Closure VecTree -> Closure VecTree -> Par (Closure VecTree)
reduce_f = \a_clo b_clo -> return $ toClosure (MkNode (unClosure a_clo) (unClosure b_clo))

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
initrand seed = when (seed /= 0) $ setStdGen (mkStdGen seed)

-- parse (optional) arguments in this order:
-- * version to run
-- * X value for Mandel
-- * Y value for Mandel
-- * Depth value for Mandel
-- * Threshold for Mandel
parseArgs :: [String] -> (Int, Int, Int, Int, Int, Int)
parseArgs []     = (defVers, defX, defY, defDepth,defThreshold,defExpected)
parseArgs (s:ss) =
  let go :: Int -> [String] -> (Int, Int, Int, Int,Int, Int)
      go v []              = (v, defX, defY, defDepth,defThreshold,defExpected)
      go v [s1]            = (v, defX, read s1,  defDepth,defThreshold,defExpected)
      go v [s1,s2]         = (v, read s1,  read s2,  defDepth,defThreshold,defExpected)
      go v [s1,s2,s3]      = (v, read s1,  read s2,  read s3, defThreshold,defExpected)
      go v [s1,s2,s3,s4] = (v, read s1,  read s2,  read s3, read s4,defExpected)
      go v (s1:s2:s3:s4:s5:_) = (v, read s1,  read s2,  read s3, read s4, read s5)
  in case stripPrefix "v" s of
       Just s' -> go (read s') ss
       Nothing -> go defVers (s:ss)

-- defaults from Simon Marlow from monad-par example
defVers, defX, defY, defDepth, defThreshold, defExpected :: Int
defVers      = 0
defX         = 1024
defY         = 1024
defDepth     = 256
defThreshold = 1
defExpected  = 0

main :: IO ()
main = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering
  register declareStatic
  opts_args <- getArgs
  let (conf, seed, args) = parseOpts opts_args
  let (version, valX, valY, valDepth,valThreshold,expected) = parseArgs args
  initrand seed
  case version of
      0 -> do (pixels, t) <- timeIO $ evaluate =<< runParIO conf
                (dist_skel_par_mandel valX valY valDepth valThreshold)
              if isJust pixels
               then
                 putStrLn $
                "{v0} mandel-par " ++
                "X=" ++ show valX ++ " Y=" ++ show valY ++
                " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                " checksum=" ++ show (checkSum (fromJust pixels)) ++
                " {runtime=" ++ show t ++ "}"
               else return ()
      1 -> do (pixels, t) <- timeIO $ evaluate =<< runParIO conf
                (dist_skel_push_mandel valX valY valDepth valThreshold)
              if isJust pixels
               then
                 putStrLn $
                "{v1} mandel-push " ++
                "X=" ++ show valX ++ " Y=" ++ show valY ++
                " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                " checksum=" ++ show (checkSum (fromJust pixels)) ++
                " {runtime=" ++ show t ++ "}"
               else return ()
      2 -> do if (chaosMonkey conf)
               then chaosMonkeyUnitTest conf "mandel-parMapReduceFT" expected (checkSum <$> ft_dist_skel_par_mandel valX valY valDepth valThreshold)
               else do
                (pixels, t) <- timeIO $ evaluate =<< runParIO conf
                  (ft_dist_skel_par_mandel valX valY valDepth valThreshold)
                if isJust pixels
                 then  putStrLn $
                   "{v2} mandel-ft-par " ++
                   "X=" ++ show valX ++ " Y=" ++ show valY ++
                   " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                   " checksum=" ++ show (checkSum (fromJust pixels)) ++
                   " {runtime=" ++ show t ++ "}"
                 else return ()
      3 -> do if (chaosMonkey conf)
               then chaosMonkeyUnitTest conf "mandel-pushMapReduceFT" expected (checkSum <$> ft_dist_skel_push_mandel valX valY valDepth valThreshold)
               else do
                 (pixels, t) <- timeIO $ evaluate =<< runParIO conf
                   (ft_dist_skel_push_mandel valX valY valDepth valThreshold)
                 if isJust pixels
                  then putStrLn $
                    "{v3} mandel-ft-push " ++
                    "X=" ++ show valX ++ " Y=" ++ show valY ++
                    " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                    " checksum=" ++ show (checkSum (fromJust pixels)) ++
                    " {runtime=" ++ show t ++ "}"
                  else return ()
      4 -> do (pixels, t) <- timeIO $ evaluate =<< return (MonadPar.runPar
                (monadpar_runMandel valX valY valDepth valThreshold))
              putStrLn $
                "{v4} monadpar-mandel-par " ++
                "X=" ++ show valX ++ " Y=" ++ show valY ++
                " depth=" ++ show valDepth ++ " threshold=" ++ show valThreshold ++
                " checksum=" ++ show (checkSum pixels) ++
                " {runtime=" ++ show t ++ "}"
      _ -> return ()

