package dk.aau.sesame.olap;
/**
 * Created by kim on 2/20/14.
 */
import org.openrdf.model.*;
import org.openrdf.query.*;
import org.openrdf.repository.*;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.*;
import org.openrdf.OpenRDFException;

import java.io.File;

import org.openrdf.sail.SailException;
import org.openrdf.sail.nativerdf.NativeStore;

import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;

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

    public static void main(String args[]) throws RepositoryException, SailException, MalformedQueryException {
        if(args.length != 3)
        {
            printUsage();
            return;
        }
        RDFParserRegistry.getInstance().add(new TurtleParserFactory());
        if(args[0].equals("--load"))
        {
            String dataDir = args[1];
            String inputFile = args[2];

            if((new File(inputFile)).isDirectory())
            {
                loadDataDirectory(new File(dataDir),inputFile);
            }
            else
            {
                loadDataProgramticChunking(new File(dataDir),inputFile);
            }
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

    private static void printUsage() {
        System.out.println("whatever [--load | --query] data-dir [file-name | query]");
    }

    private static void loadDataProgramticChunking(File dataDir,String inputFile) throws RepositoryException {
        File file = new File(inputFile);
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        //con.setAutoCommit(false);

        RDFParser parser = Rio.createParser(RDFFormat.forFileName(inputFile));
        parser.setRDFHandler(new ChunkCommitter(con,false));

        try
        {
            BufferedInputStream is = new BufferedInputStream(new FileInputStream(file),10*1024*1024); //10 MB
            parser.parse(is,"");
            //con.commit();
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

    private static void loadDataDirectory(File dataDir,String inputFileDir) throws RepositoryException {
        File directory = new File(inputFileDir);
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        try
        {
            int i = 0;
            File[] files = directory.listFiles();
            long start = System.nanoTime();
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

    private static void loadData(File dataDir,String inputFile) throws RepositoryException {
        File file = new File(inputFile);
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        try
        {
            long start = System.nanoTime();
            con.add(file, null, RDFFormat.TURTLE);
            con.commit();
            System.out.println("Data loaded in :" + (System.nanoTime()-start)/1000000 + " ms");
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

    private static void readData(File dataDir,String query) throws RepositoryException {
        org.openrdf.repository.Repository repo = null;
        repo = new SailRepository(new NativeStore(dataDir, indexes));

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
