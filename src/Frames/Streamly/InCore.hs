{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-|
Module      : Frames.Streamly.InCore
Description : Support for transformations between streamly streams and Frames Array of Structures (SoA).
Copyright   : (c) Adam Conner-Sax 2020
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

This module can be used in-place of Frames.InCore.  The pipes functions are all replaced by equivalent streamly functions.
Relevant classes and type-families are re-exported for convenience.
-}
module Frames.Streamly.InCore
    (
      inCoreSoA
    , inCoreAoS
    , inCoreAoS'
    , inCoreSoA_F
    , inCoreAoS_F
    , inCoreAoS'_F
    -- * Re-exports from Frames
    , toAoS
    , VectorFor
    , VectorMFor
    , VectorMs
    , Vectors
    , RecVec(..)    
    
    )
where

import qualified Streamly                               as Streamly
import qualified Streamly.Prelude                       as Streamly
import qualified Streamly.Data.Fold                     as Streamly.Fold
import qualified Streamly.Internal.Data.Fold            as Streamly.Fold

import qualified Control.Monad.Primitive                as Prim

import qualified Data.Vinyl                             as Vinyl

import qualified Frames                                 as Frames
import qualified Frames.InCore                          as Frames
import           Frames.InCore                           (VectorFor, VectorMFor, VectorMs, Vectors, RecVec(..), toAoS)

import Data.Proxy (Proxy(..))


-- | Fold a stream of 'Vinyl' records into SoA (Structure-of-Arrays) form.
-- Here as a 'streamly' fold, so it may be deployed along with other folds or on only part of a stream.
inCoreSoA_F :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.Fold.Fold m (Frames.Record rs) (Int, Vinyl.Rec (((->) Int) Frames.:. Frames.ElField) rs)
inCoreSoA_F = Streamly.Fold.mkFold feed initial fin
  where feed (!i, !sz, !mvs') row
          | i == sz = Frames.growRec (Proxy::Proxy rs) mvs'
                      >>= flip feed row . (i, sz*2,)
          | otherwise = do Frames.writeRec (Proxy::Proxy rs) i mvs' row
                           return (i+1, sz, mvs')
                         
        initial = do
          mvs <- Frames.allocRec (Proxy :: Proxy rs) Frames.initialCapacity
          return (0, Frames.initialCapacity, mvs)
          
        fin (n, _, mvs') =
          do vs <- Frames.freezeRec (Proxy::Proxy rs) n mvs'
             return . (n,) $ Frames.produceRec (Proxy::Proxy rs) vs
{-# INLINE inCoreSoA_F #-}

-- | Perform the 'inCoreSoA_F' fold on a stream of records.
inCoreSoA :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.SerialT m (Frames.Record rs)
          -> m (Int, Vinyl.Rec (((->) Int) Frames.:. Frames.ElField) rs)
inCoreSoA = Streamly.fold inCoreSoA_F
{-# INLINE inCoreSoA #-}

-- | Fold a stream of 'Vinyl' records into AoS (Array-of-Structures) form.
inCoreAoS_F :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.Fold.Fold m (Frames.Record rs) (Frames.FrameRec rs)
inCoreAoS_F = fmap (uncurry Frames.toAoS) inCoreSoA_F
{-# INLINE inCoreAoS_F #-}

-- | Perform the 'inCoreAoS_F' fold on a stream of records.
inCoreAoS :: forall m rs. (Prim.PrimMonad m, Frames.RecVec rs)
          => Streamly.SerialT m (Frames.Record rs)
          -> m (Frames.FrameRec rs)
inCoreAoS = Streamly.fold inCoreAoS_F --fmap (uncurry Frames.toAoS) . inCoreSoA
{-# INLINE inCoreAoS #-}


-- | More general AoS fold, allowing for a, possible column changing, transformation of the records while in SoA form.
inCoreAoS'_F ::  forall ss rs m. (Prim.PrimMonad m, Frames.RecVec rs)
           => (Frames.Rec ((->) Int Frames.:. Frames.ElField) rs -> Frames.Rec ((->) Int Frames.:. Frames.ElField) ss)
           -> Streamly.Fold.Fold m (Frames.Record rs) (Frames.FrameRec ss)
inCoreAoS'_F f  = fmap (uncurry Frames.toAoS . aux) inCoreSoA_F
  where aux (x,y) = (x, f y)
{-# INLINE inCoreAoS'_F #-}  

-- | Perform the more general AoS fold on a stream of records.
inCoreAoS' ::  forall ss rs m. (Prim.PrimMonad m, Frames.RecVec rs)
           => (Frames.Rec ((->) Int Frames.:. Frames.ElField) rs -> Frames.Rec ((->) Int Frames.:. Frames.ElField) ss)
           -> Streamly.SerialT m (Frames.Record rs)
           -> m (Frames.FrameRec ss)
inCoreAoS' f = Streamly.fold (inCoreAoS'_F f)
{-# INLINE inCoreAoS' #-}  

