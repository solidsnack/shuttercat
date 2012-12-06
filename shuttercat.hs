{-# LANGUAGE OverloadedStrings #-}

import           Prelude hiding (lines)
import           Control.Applicative
import           Control.Concurrent.Async
import           Control.Concurrent.STM
import           Control.Monad
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString hiding (hPutStrLn, pack)
import qualified Data.ByteString.Char8 as ByteString hiding (breakEnd, foldl')
import           Data.Monoid
import           Data.Time
import           Data.Time.Clock ()
import           Data.Word
import           System.Exit
import           System.IO
import           System.Mem

import           Control.Concurrent.STM.ShutterChan


main :: IO ()
main  = csv 250000 stdin
 where lines = records atLastFullLine
       csv   = records csvSplit

records :: (ByteString -> (ByteString, ByteString)) -> Int -> Handle -> IO ()
records f t h = do (bytes, segments, chunks, scrap) <- atomically ctx
                   a <- async $ recv h           bytes
                   b <- async $ segment f scrap  bytes segments
                   c <- async $ handoff t              segments chunks
                   d <- async $ send                            chunks
                   mapM_ wait [a, b, c, d]
                   exitSuccess
 where ctx = (,,,) <$> newTChan <*> newTChan <*> newTChan <*> newTVar ""

octets :: Int -> Handle -> IO ()
octets t h = do (blocks, chunks) <- atomically ctx
                a <- async $ recv h     blocks
                b <- async $ handoff t  blocks chunks
                c <- async $ send              chunks
                mapM_ wait [a, b, c]
                exitSuccess
 where ctx = (,) <$> newTChan <*> newTChan

handoff t  = transfer t now performGC
 where now = (hPutStrLn stderr . show) =<< getCurrentTime

msg :: ByteString -> IO ()
msg  = ByteString.hPutStrLn stderr


recv :: Handle -> TChan (Maybe ByteString) -> IO ()
recv h o = do bytes <- ByteString.hGetSome h 16384
              ("" /= bytes) `when` atomically (writeTChan o (Just bytes))
              (eof, closed) <- (,) <$> hIsEOF h <*> hIsClosed h
              if eof || closed then atomically (writeTChan o Nothing)
                               else recv h o

send :: TChan [ByteString] -> IO ()
send i = do chunks <- atomically $ readTChan i
            mapM_ ByteString.putStr chunks
            hFlush stdout
            (chunks /= []) `when` send i


segment :: (ByteString -> (ByteString, ByteString))
        -> TVar ByteString -> TChan (Maybe ByteString)
                           -> TChan (Maybe ByteString) -> IO ()
segment split scrap i o = do
  continue <- atomically (maybe stop step =<< readTChan i)
  when continue (segment split scrap i o)
  -- NB: if the STM function handled the recursion, it would block the
  -- runtime. (Only tested with single threaded runtime.)
 where stop = False <$ writeTChan o Nothing
       step "" = return True
       step b  = do (full, rest) <- split . (<>b) <$> readTVar scrap
                    when (full /= "") (writeTChan o (Just full))
                    writeTVar scrap rest
                    return True

atLastFullLine :: ByteString -> (ByteString, ByteString)
atLastFullLine  = ByteString.breakEnd (== 0x0a)

-- Handles CSV records, which are delimited by newlines but allows quoting.
-- Thus we scan for balanced quotes.
csvSplit :: ByteString -> (ByteString, ByteString)
csvSplit b = ByteString.splitAt index b
 where (_, _, index) = ByteString.foldl' step (False, 0, 0) b

step :: (Bool, Int, Int) -> Word8 -> (Bool, Int, Int)
step (quoted, index, last) 0x22 = (not quoted, index+1, last)    -- '"'
step (False,  index, _   ) 0x0a = (False,      index+1, index+1) -- '\n'
step (quoted, index, last) _    = (quoted,     index+1, last)

