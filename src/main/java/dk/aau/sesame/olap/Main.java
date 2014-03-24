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
import java.util.Scanner;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.inferencer.fc.CustomGraphQueryInferencer;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.turtle.TurtleParserFactory;

public class Main {

    private static File testDataDir = new File("dbs/test/");
    private static File inferenceDataDir = new File("dbs/inference/");
    private static File agriBusiDataDir = new File("dbs/agri-busi/");
    private static String indexes = "spoc,posc,cosp,cspo,cpos";
    private static Options options = new Options();

    private static boolean commit;
    private static long chunkSize;

    public static void main(String args[]) throws Exception {


        options.addOption(OptionBuilder.withLongOpt("commit")
                .withDescription("commit after every CHUNK triple")
                .create());
        options.addOption(OptionBuilder.withLongOpt("no-commit")
                .withDescription("do not commit after every CHUNK triple (default)")
                .create());
        options.addOption(OptionBuilder.withLongOpt( "load" )
                .withDescription( "load INPUT-FILE into DATA-DIR, INPUT-FILE may be a directory" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "query" )
                .withDescription( "query the DATA-DIR with query specified with QUERY or INPUT-FILE" )
                .create());
        options.addOption(OptionBuilder.withLongOpt( "chunk" )
                .hasArg()
                .withArgName("size")
                .withDescription( "size of chunks measure load speed and be committed if COMMIT is set, use k and M for 1000 and 1000000 (default 1M)" )
                .create());

        if(args.length < 3)
        {
            printUsage();
            return;
        }
        String dataDir = args[args.length-2];
        String inputFile = args[args.length-1];
        RDFParserRegistry.getInstance().add(new TurtleParserFactory());
        QueryParserRegistry.getInstance().add(new SPARQLParserFactory());

        CommandLineParser parser = new PosixParser();
        CommandLine commandLine = parser.parse(options,args);

        chunkSize = Long.parseLong(commandLine.getOptionValue("chunk","1M").toUpperCase().replaceAll("K","000").replaceAll("M","000000"));
        commit = commandLine.hasOption("commit") && !commandLine.hasOption("no-commit");

        if(commandLine.hasOption("load"))
        {
            loadData(dataDir,inputFile);
        }
        else if(commandLine.hasOption("query"))
        {
            readData(dataDir,inputFile);
        }
        else
        {
            printUsage();
        }
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

    private static void loadData(String dataDirName,String inputFileName) throws Exception
    {
        File inputFile = new File(inputFileName);
        File dataDir = new File(dataDirName);

        if(inputFile.isDirectory())
        {
            loadDataDirectory(dataDir,inputFile);
        }
        else
        {
            if(!inputFile.isFile())
            {
                throw new IllegalArgumentException("Invalid input file specified");
            }
            loadDataProgramticChunking(dataDir,inputFile);
        }
    }

    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sesam [OPTIONS]* DATA-DIR [INPUT-FILE | QUERY]", options);
    }

    private static void loadDataProgramticChunking(File dataDir,File inputFile) throws RepositoryException {
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        //con.setAutoCommit(false);

        RDFParser parser = Rio.createParser(RDFFormat.forFileName(inputFile.getName()));
        parser.setRDFHandler(new ChunkCommitter(con,commit,chunkSize));

        try
        {
            BufferedInputStream is = new BufferedInputStream(new FileInputStream(inputFile),10*1024*1024); //10 MB
            parser.parse(is,"");
        }
        catch (RDFParseException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (RDFHandlerException e) {
            e.printStackTrace();
        } finally {
            con.close();
            repo.shutDown();
        }
    }

    private static void loadDataDirectory(File dataDir,File inputFileDir) throws RepositoryException {
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        try
        {
            int i = 0;
            File[] files = inputFileDir.listFiles();
            long start = System.nanoTime();
            con.begin();
            for (File file: files)  
            {
                String fileName = file.getAbsolutePath();
                con.add(file, null, RDFFormat.NTRIPLES);
                ++i;
                System.out.println("Files processed: " + i + "/" + files.length + ". Time: " + (System.nanoTime()-start)/1000000 + " ms");
            }
            con.commit();
        }
        catch (RDFParseException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            con.close();
            repo.shutDown();
        }
    }

    private static void readData(String dataDir,String query) throws RepositoryException, FileNotFoundException {

        File queryFile = new File(query);
        if(queryFile.exists())
        {
            if(queryFile.isDirectory())
            {
                for(File file : queryFile.listFiles())
                {
                    readData(dataDir, file.getAbsolutePath());
                }
            }
            else if(queryFile.canRead())
            {
                query = new Scanner(queryFile).useDelimiter("\\Z").next();
                readData(dataDir, query);
            }
            else
            {
                throw new IllegalArgumentException("Unable to read file: '" + query + "' for query");
            }
            return;
        }

        org.openrdf.repository.Repository repo = null;
        repo = new SailRepository(new NativeStore(new File(dataDir), indexes));

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
                if(i < 100)
                {
                    for (String name : bindingNames)
                    {
                        System.out.print(bindingSet.getValue(name) + " ");
                    }
                    System.out.println();
                }
                ++i;
            }
            System.out.println(i);
        }
        catch (OpenRDFException e) {
            e.printStackTrace();
        }
        finally {
            con.close();
            repo.shutDown();
        }
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
