{-# LANGUAGE DeriveLift #-}

module Frames.Streamly.Streaming.Common where


import  Language.Haskell.TH.Syntax (Lift)
import qualified Data.Text as T

data Separator = CharSeparator Char | TextSeparator Text deriving (Show, Eq, Ord, Lift)

defaultSep :: Separator
defaultSep = CharSeparator ','
{-# INLINE defaultSep #-}

type QuoteChar = Char

data QuotingMode
    -- | No quoting enabled. The separator may not appear in values
  = NoQuoting
    -- | Quoted values with the given quoting character. Quotes are escaped by doubling them.
    -- Mostly RFC4180 compliant, except doesn't support newlines in values
  | RFC4180Quoting QuoteChar
  deriving (Eq, Show, Lift)

defaultQuotingMode :: QuotingMode
defaultQuotingMode = RFC4180Quoting '\"'
{-# INLINE defaultQuotingMode #-}

separatorToText :: Separator -> Text
separatorToText (CharSeparator c) = T.singleton c
separatorToText (TextSeparator t) = t
{-# INLINEABLE separatorToText #-}

textToSeparator :: Text -> Separator
textToSeparator t = if T.length t == 1 then CharSeparator (T.head t) else TextSeparator t
{-# INLINEABLE textToSeparator #-}

-- | Helper to split a 'T.Text' on commas and strip leading and
-- trailing whitespace from each resulting chunk.
tokenizeRow :: Separator -> QuotingMode -> T.Text -> [T.Text]
tokenizeRow sep quoting = handleQuoting . splitRow sep
  where
    {-# INLINE handleQuoting #-}
    handleQuoting = case quoting of
      NoQuoting-> id
      RFC4180Quoting quote -> reassembleRFC4180QuotedParts (separatorToText sep) quote

splitRow :: Separator -> T.Text -> [T.Text]
splitRow sep = case sep of
  CharSeparator c -> T.split (== c)
  TextSeparator t -> T.splitOn t
{-# INLINE splitRow #-}

-- | Post processing applied to a list of tokens split by the
-- separator which should have quoted sections reassembeld
reassembleRFC4180QuotedParts :: Text -> Char -> [T.Text] -> [T.Text]
reassembleRFC4180QuotedParts sep quoteChar = go
  where go [] = []
        go (part:parts)
          | T.null part = T.empty : go parts
          | prefixQuoted part =
            if suffixQuoted part
            then unescape (T.drop 1 . T.dropEnd 1 $ part) : go parts
            else case break suffixQuoted parts of
                   (h,[]) -> [unescape (T.intercalate sep (T.drop 1 part : h))]
                   (h,t:ts) -> unescape
                                 (T.intercalate
                                    sep
                                    (T.drop 1 part : h ++ [T.dropEnd 1 t]))
                               : go ts
          | otherwise = T.strip part : go parts
        {-# INLINE prefixQuoted #-}
        prefixQuoted t =
          T.head t == quoteChar &&
          T.length (T.takeWhile (== quoteChar) t) `rem` 2 == 1
        {-# INLINE suffixQuoted #-}
        suffixQuoted t =
          quoteText `T.isSuffixOf` t &&
          T.length (T.takeWhileEnd (== quoteChar) t) `rem` 2 == 1
        {-# INLINE quoteText #-}
        quoteText = T.singleton quoteChar
        {-# INLINE unescape #-}
        unescape :: T.Text -> T.Text
        unescape = T.replace q2 quoteText
          where q2 = quoteText <> quoteText
