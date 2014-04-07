package dk.aau.sesame.olap;
/**
 * Created by kim on 2/20/14.
 */
import org.apache.commons.cli.*;
import org.openrdf.model.*;
import org.openrdf.query.*;
import org.openrdf.query.parser.QueryParserRegistry;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.*;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.*;
import org.openrdf.OpenRDFException;

import java.io.*;

import org.openrdf.sail.SailException;
import org.openrdf.sail.nativerdf.NativeStore;

import java.util.List;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.inferencer.fc.CustomGraphQueryInferencer;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.turtle.TurtleParserFactory;

public class Main
{
    private static String indexes = "spoc,posc";
    private static String inputDataDir = "";
    private static Options options = new Options();

    private static boolean commit;
    private static long chunkSize;

    public static void main(String args[]) throws Exception
    {
        options.addOption(OptionBuilder.withLongOpt("commit")
                .withDescription("commit after every CHUNK triple")
                .create());
        options.addOption(OptionBuilder.withLongOpt("test")
                .withDescription("run a test query in DATA-DIR repository")
                .create());
        options.addOption(OptionBuilder.withLongOpt("help")
                .withDescription("show help text")
                .create());
        options.addOption(OptionBuilder.withLongOpt("no-commit")
                .withDescription("do not commit after every CHUNK triple (default)")
                .create());
        options.addOption(OptionBuilder.withLongOpt( "load" )
                .hasArg()
                .withArgName("input-file | input-dir | construct-query")
                .withDescription( "load INPUT-FILE or files in INPUT-DIR into DATA-DIR" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "query" )
                .hasArg()
                .withArgName("query-file | query-dir | select-query")
                .withDescription( "query the DATA-DIR with query specified by SELECT-QUERY, in QUERY-FILE or in all files in QUERY-DIR" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "construct" )
                .hasArg()
                .withArgName("query-file | query-dir | construct-query")
                .withDescription( "run construct query on INPUT-REPO and load resulting triples back into DATA-DIR" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "chunk" )
                .hasArg()
                .withArgName("size")
                .withDescription( "size of chunks measure load speed and be committed if COMMIT is set, use k and M for 1000 and 1000000 (default 1M)" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "index" )
                .hasArg()
                .withArgName("indexes")
                .withDescription( "comma separated indexes, e.g. spoc is a subject-predicate-object-context index (default '"+indexes+"')" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "input-repo" )
                .hasArg()
                .withArgName("repository")
                .withDescription( "input repository used for construct (default is DATA-DIR)")
                .create());

        if(args.length == 0)
        {
            printUsage();
            return;
        }
        String dataDir = args[args.length-1];
        RDFParserRegistry.getInstance().add(new TurtleParserFactory());
        QueryParserRegistry.getInstance().add(new SPARQLParserFactory());

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = parser.parse(options,args);

        chunkSize = Long.parseLong(commandLine.getOptionValue("chunk","1M").toUpperCase().replaceAll("K","000").replaceAll("M","000000"));
        indexes = commandLine.getOptionValue("index",indexes);
        inputDataDir = commandLine.getOptionValue("input-repo",inputDataDir);
        commit = commandLine.hasOption("commit") && !commandLine.hasOption("no-commit");

        if(commandLine.hasOption("help"))
        {
            printUsage();
            return;
        }

        int i = commandLine.hasOption("load") ? 1 : 0;
        i += commandLine.hasOption("construct") ? 1 : 0;
        i += commandLine.hasOption("query") ? 1 : 0;
        i += commandLine.hasOption("test") ? 1 : 0;
        if(i > 1)
        {
            System.out.println("Too many modes given");
            printUsage();
            return;
        }

        Mode mode = null;
        String arg = null;
        if(commandLine.hasOption("load"))
        {
            mode = new LoadMode(commit,chunkSize, indexes);
            arg = commandLine.getOptionValue("load");
        }
        else if(commandLine.hasOption("construct"))
        {
            mode = new ConstructMode(commit, indexes, inputDataDir,chunkSize);
            arg = commandLine.getOptionValue("construct");
        }
        else if(commandLine.hasOption("query"))
        {
            mode = new QueryMode(indexes);
            arg = commandLine.getOptionValue("query");
        }
        else if(commandLine.hasOption("test"))
        {
            mode = new QueryMode(indexes);
            arg = "select (count(*) as ?count) where {?S ?P ?O}";
        }
        else
        {
            printUsage();
            return;
        }
        mode.handle(dataDir, arg);
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sesam [OPTIONS...] DATA-DIR\nspecify either LOAD, QUERY, TEST, or CONSTRUCT", options);
    }
}
