{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-|
Module      : Frames.Streamly.Transform
Description : Support for transformations on Frames using streamly streams as an intermediate structure.
Copyright   : (c) Adam Conner-Sax 2020
License     : BSD-3-Clause
Maintainer  : adam_conner_sax@yahoo.com
Stability   : experimental

This module adds some functions for using streamly to make transformations on Frames that are either:

1. Like filtering or some one-to-many mapping of rows,
whic makes the efficient memory layout useless in-flight.  Frames already implements 'filterFrame'
this way, using Pipes and the ST monad.

2. Expensive enough per row that the concurrency features of streams become worthwhile.
-}
module Frames.Streamly.Transform
    ( transform
    , frameConcat
    , filter
    , concurrentMapM
    , mapMaybe
    , concurrentMapMaybeM
    )
where

import qualified Frames.Streamly.InCore as FS
import Frames.Streamly.Streaming.Streamly (StreamlyStream(..))
import Prelude hiding (filter, mapMaybe)

#if MIN_VERSION_streamly(0,9,0)
import qualified Streamly.Data.Stream as Streamly
import qualified Streamly.Data.StreamK as StreamK
import qualified Streamly.Data.Stream.Prelude as Streamly
#elif MIN_VERSION_streamly(0,8,0)
import Streamly.Prelude                       (IsStream)
import qualified Streamly.Prelude                       as Streamly
#else
import qualified Streamly.Prelude                       as Streamly
import qualified Streamly                               as Streamly
import           Streamly                                ( IsStream )
#endif

import qualified Control.Monad.Primitive                as Prim
import Control.Monad.ST (runST)
import qualified Frames

#if MIN_VERSION_streamly(0,9,0)
#elif MIN_VERSION_streamly(0,8,0)
fromSerial :: IsStream t => Streamly.SerialT m a -> t m a
fromSerial = Streamly.fromSerial
{-# INLINE fromSerial #-}

fromAhead :: IsStream t => Streamly.AheadT m a -> t m a
fromAhead = Streamly.fromAhead
{-# INLINE fromAhead #-}
#else
fromSerial :: IsStream t => Streamly.SerialT m a -> t m a
fromSerial = Streamly.serially
{-# INLINE fromSerial #-}

fromAhead :: IsStream t => Streamly.AheadT m a -> t m a
fromAhead = Streamly.aheadly
{-# INLINE fromAhead #-}
#endif



#if MIN_VERSION_streamly(0,9,0)
  -- | Use streamly to transform a frame.
transform ::
  forall as bs m.
  (
  Prim.PrimMonad m
  , FS.RecVec as
  , FS.RecVec bs
  )
  => (Streamly.Stream m (Frames.Record as) -> Streamly.Stream m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
transform f = FS.inCoreAoS . StreamlyStream . f . StreamK.toStream . StreamK.fromFoldable --Streamly.fromFoldable
{-# INLINE transform #-}

-- | Filter using streamly
filter :: FS.RecVec as => (Frames.Record as -> Bool) -> Frames.FrameRec as -> Frames.FrameRec as
filter f frame = runST $ transform (Streamly.filter f) frame
{-# INLINE filter #-}

-- | map using speculative streams (concurrency that preserves ordering of results).
concurrentMapM :: (Streamly.MonadAsync m
                  , Prim.PrimMonad m
                  , FS.RecVec as
                  , FS.RecVec bs
                  ) => (Frames.Record as -> m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapM f = transform (Streamly.parMapM (Streamly.ordered True) f)
{-# INLINE concurrentMapM #-}

-- | mapMaybe using streamly
mapMaybe :: (FS.RecVec as
            , FS.RecVec bs
            ) => (Frames.Record as -> Maybe (Frames.Record bs)) -> Frames.FrameRec as -> Frames.FrameRec bs
mapMaybe f frame = runST $ transform (Streamly.mapMaybe f) frame
{-# INLINE mapMaybe #-}

-- | mapMaybeM using speculative streams (concurrency that preserves ordering of results).
concurrentMapMaybeM :: (Prim.PrimMonad m
                       , Streamly.MonadAsync m
                       , FS.RecVec as
                       , FS.RecVec bs
                       ) => (Frames.Record as -> m (Maybe (Frames.Record bs))) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapMaybeM f = transform (Streamly.catMaybes . Streamly.parMapM (Streamly.ordered True) f)
{-# INLINE concurrentMapMaybeM #-}

frameConcat :: (FS.RecVec rs, Foldable f, Functor f) => f (Frames.FrameRec rs) -> Frames.FrameRec rs
frameConcat x = if length x < 500
                then mconcat $ toList x
                else runST $ FS.inCoreAoS .  StreamlyStream . Streamly.concatMap (StreamK.toStream .  StreamK.fromFoldable)  $  StreamK.toStream $ StreamK.fromFoldable x
{-# INLINEABLE frameConcat #-}



#else
-- | Use streamly to transform a frame.
transform ::
  forall t as bs m.
  (
    IsStream t
  , Prim.PrimMonad m
  , FS.RecVec as
  , FS.RecVec bs
  )
  => (t m (Frames.Record as) -> Streamly.SerialT m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
transform f = FS.inCoreAoS . StreamlyStream . f . Streamly.fromFoldable
{-# INLINE transform #-}

-- | Filter using streamly
filter :: FS.RecVec as => (Frames.Record as -> Bool) -> Frames.FrameRec as -> Frames.FrameRec as
filter f frame = runST $ transform (fromSerial . Streamly.filter f) frame
{-# INLINE filter #-}

-- | map using speculative streams (concurrency that preserves ordering of results).
concurrentMapM :: (Prim.PrimMonad m
                  , Streamly.MonadAsync m
                  , FS.RecVec as
                  , FS.RecVec bs
                  ) => (Frames.Record as -> m (Frames.Record bs)) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapM f = transform (fromAhead . Streamly.mapM f)
{-# INLINE concurrentMapM #-}

-- | mapMaybe using streamly
mapMaybe :: (FS.RecVec as
            , FS.RecVec bs
            ) => (Frames.Record as -> Maybe (Frames.Record bs)) -> Frames.FrameRec as -> Frames.FrameRec bs
mapMaybe f frame = runST $ transform (fromAhead . Streamly.mapMaybe f) frame
{-# INLINE mapMaybe #-}

-- | mapMaybeM using speculative streams (concurrency that preserves ordering of results).
concurrentMapMaybeM :: (Prim.PrimMonad m
                       , Streamly.MonadAsync m
                       , FS.RecVec as
                       , FS.RecVec bs
                       ) => (Frames.Record as -> m (Maybe (Frames.Record bs))) -> Frames.FrameRec as -> m (Frames.FrameRec bs)
concurrentMapMaybeM f = transform (fromAhead . Streamly.mapMaybeM f)
{-# INLINE concurrentMapMaybeM #-}
#endif
