module Scientific.Workflow.Main.Options
    ( CMD (..)
    , GlobalOpts(..)
    , argsParser
    ) where

import           Data.List.Split     (splitOn)
import           Data.Semigroup      ((<>))
import           Options.Applicative

argsParser :: String -> ParserInfo CMD
argsParser h = info (helper <*> parser) $ fullDesc <> header h
  where
    parser = subparser $ (
        command "run" (info (helper <*> runParser) $
            fullDesc <> progDesc "run workflow")
     <> command "view" (info (helper <*> viewParser) $
            fullDesc <> progDesc "view workflow")
     <> command "cat" (info (helper <*> catParser) $
            fullDesc <> progDesc "display the result of a node")
     <> command "write" (info (helper <*> writeParser) $
            fullDesc <> progDesc "write the result to a node")
     <> command "rm" (info (helper <*> rmParser) $
            fullDesc <> progDesc "delete the result of a node.")
     <> command "recover" (info (helper <*> recoverParser) $
            fullDesc <> progDesc "Recover database from backup.")
     <> command "backup" (info (helper <*> dumpDBParser) $
            fullDesc <> progDesc "Backup database.")
     <> command "execFunc" (info (helper <*> callParser) $
            fullDesc <> progDesc "Do not call this directly.")
     )

data CMD = Run GlobalOpts Int Bool (Maybe [String]) (Maybe String)
         | View Bool
         | Cat GlobalOpts String
         | Write GlobalOpts String FilePath
         | Delete GlobalOpts String
         | Recover GlobalOpts FilePath
         | DumpDB GlobalOpts FilePath
         | Call GlobalOpts String String String

data GlobalOpts = GlobalOpts
    { dbPath     :: FilePath
    , configFile :: Maybe [FilePath]
    }

globalParser :: Parser GlobalOpts
globalParser = GlobalOpts
           <$> strOption
               ( long "db-path"
              <> value "sciflow.db"
              <> metavar "DB_PATH" )
           <*> (optional . option (splitOn "," <$> str))
               ( long "config"
              <> metavar "CONFIG_PATH" )

runParser :: Parser CMD
runParser = Run
    <$> globalParser
    <*> option auto
        ( short 'N'
       <> value 1
       <> metavar "CORES"
       <> help "The number of concurrent processes." )
    <*> switch
        ( long "remote"
       <> help "Submit jobs to remote machines.")
    <*> (optional . option (splitOn "," <$> str))
        ( long "select"
       <> metavar "SELECTED"
       <> help "Run only selected nodes.")
    <*> (optional . fmap f . strOption)
        ( long "log-server"
       <> metavar "Log_SERVER" )
  where
    f x = case x of
        ('\\' : '0' : rest) -> '\0' : rest
        x' -> x'

viewParser :: Parser CMD
viewParser = View <$> switch (long "raw")

catParser :: Parser CMD
catParser = Cat
        <$> globalParser
        <*> strArgument
            (metavar "NODE_ID")

writeParser :: Parser CMD
writeParser = Write
          <$> globalParser
          <*> strArgument
              (metavar "NODE_ID")
          <*> strArgument
              (metavar "INPUT_FILE")

rmParser :: Parser CMD
rmParser = Delete
       <$> globalParser
       <*> strArgument
           (metavar "NODE_ID")

recoverParser :: Parser CMD
recoverParser = Recover
            <$> globalParser
            <*> strArgument
                (metavar "BACKUP")

dumpDBParser :: Parser CMD
dumpDBParser = DumpDB
           <$> globalParser
           <*> strArgument
               (metavar "OUTPUT_DIR")

callParser :: Parser CMD
callParser = Call
         <$> globalParser
         <*> strArgument mempty
         <*> strArgument mempty
         <*> strArgument mempty
