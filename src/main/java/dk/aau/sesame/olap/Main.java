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

        String dataDir = args[args.length-1];
        RDFParserRegistry.getInstance().add(new TurtleParserFactory());
        QueryParserRegistry.getInstance().add(new SPARQLParserFactory());

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = parser.parse(options,args);

        chunkSize = Long.parseLong(commandLine.getOptionValue("chunk","1M").toUpperCase().replaceAll("K","000").replaceAll("M","000000"));
        indexes = commandLine.getOptionValue("index",indexes);
        inputDataDir = commandLine.getOptionValue("input-repo",inputDataDir);
        commit = commandLine.hasOption("commit") && !commandLine.hasOption("no-commit");

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
            mode = new ConstructMode(commit, indexes, inputDataDir);
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

        //loadData(testDataDir,"test.ttl");
        //readData(testDataDir,"SELECT ?x ?y WHERE { ?x ?p ?y } LIMIT 10");
        //loadData(agriBusiDataDir, "../../Documents/10sem/sw10/procedures/agri.nt");
        //loadDataDirectory(agriBusiDataDir, "../../Documents/10sem/sw10/procedures/agri");
        //readData(agriBusiDataDir, "SELECT ?x ?y WHERE { ?x ?p ?y } LIMIT 10");
        //readData(testDataDir, "PREFIX fn: <http://example.org/custom-function/>"+
        //    "SELECT ?x WHERE { ?x rdf:label ?label."+
        //    "FILTER(fn:palindrome(?label)) } LIMIT 10");
        //loadInferredData(inferenceDataDir, "ssb-inf-10k.ttl");
        //readData(inferenceDataDir, "select * where {?x rdf:type <http://class-a>}");
        //readData(inferenceDataDir, "prefix rdfh: <http://lod2.eu/schemas/rdfh#>" +
        //        " prefix rdfh-inst: <http://lod2.eu/schemas/rdfh-inst#> " +
        //        " select * where {rdfh-inst:lineorder_1_2 ?P ?O}");
        //readData(inferenceDataDir, "prefix rdfh: <http://lod2.eu/schemas/rdfh#>" +
        //        " prefix rdfh-inst: <http://lod2.eu/schemas/rdfh-inst#> " +
        //        " select (count(*) as ?count) where {?S ?P ?O}");
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sesam [OPTIONS...] DATA-DIR\nspecify either LOAD, QUERY, TEST, or CONSTRUCT", options);
    }

    private static void loadInferredData(File dataDir, String inputFile) throws RepositoryException, SailException, MalformedQueryException {
        File file = new File(inputFile);
        org.openrdf.repository.Repository repo = new SailRepository
        (
            new ForwardChainingRDFSInferencer
            (
                                                                                                    new CustomGraphQueryInferencer
                (
                    new NativeStore(dataDir, indexes),

                    QueryLanguage.SPARQL,

                    "prefix rdfh: <http://lod2.eu/schemas/rdfh#> " +
                            "prefix rdfh-inst: <http://lod2.eu/schemas/rdfh-inst#> " +
                            "CONSTRUCT {?fact ?levelProp ?level} " +
                            "WHERE {" +
                            " ?fact ?dimProp ?dim . " +
                            " ?dim a rdfh:Dimension . " +
                            " ?dim ?levelProp ?level . " +
                            "}",

                        "prefix rdfh: <http://lod2.eu/schemas/rdfh#> " +
                                "prefix rdfh-inst: <http://lod2.eu/schemas/rdfh-inst#> " +
                                "CONSTRUCT {?fact ?dimProp ?dim . " +
                                " ?dim ?levelPro ?level} " +
                                "WHERE {" +
                                " ?dim a rdfh:Dimension . " +
                                " ?fact ?dimProp ?dim . " +
                                " ?dim ?levelProp ?level . " +
                                "}"
                )
            )
        );
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        con.begin();

        try
        {
            long start = System.nanoTime();
            con.add(file, null, RDFFormat.TURTLE);
            con.commit();
            System.out.println("Data loaded in: " + (System.nanoTime() - start) / 1000000 + " ms");
        }
        catch (RDFParseException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        } finally {
            con.close();
            repo.shutDown();
        }
    }

    private static void readInferredData(File dataDir,String query) throws RepositoryException, SailException, MalformedQueryException {
        //org.openrdf.repository.Repository repo = new SailRepository(new CustomGraphQueryInferencer(new NativeStore(dataDir, indexes),QueryLanguage.register("SPARQL"),"",""));
        org.openrdf.repository.Repository repo = new SailRepository(
                new ForwardChainingRDFSInferencer(
                    new NativeStore(dataDir, indexes)
                )
        );

        repo.initialize();

        RepositoryConnection con = repo.getConnection();
        try
        {
            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);

            TupleQueryResult result = tupleQuery.evaluate();

            List<String> bindingNames = result.getBindingNames();
            int i = 0;
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                Value firstValue = bindingSet.getValue(bindingNames.get(0));

                System.out.println(firstValue);
                ++i;
            }
            System.out.println(i);
        }
        catch (OpenRDFException e) {
            e.printStackTrace();
        }
        finally {
            con.close();
        }
    }
}
