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

public class Main {

    private static File testDataDir = new File("dbs/test/");
    private static File inferenceDataDir = new File("dbs/inference/");
    private static File agriBusiDataDir = new File("dbs/agri-busi/");
    private static String indexes = "spoc,posc,cosp,cspo,cpos";

    public static void main(String args[]) throws RepositoryException, SailException, MalformedQueryException {
        //loadData(testDataDir,"test.ttl");
        //readData(testDataDir,"SELECT ?x ?y WHERE { ?x ?p ?y } LIMIT 10");
        //loadData(agriBusiDataDir, "../../Documents/10sem/sw10/procedures/agri.nt");
        //loadDataDirectory(agriBusiDataDir, "../../Documents/10sem/sw10/procedures/agri");
        //readData(agriBusiDataDir, "SELECT ?x ?y WHERE { ?x ?p ?y } LIMIT 10");
        //readData(testDataDir, "PREFIX fn: <http://example.org/custom-function/>"+
        //    "SELECT ?x WHERE { ?x rdf:label ?label."+
        //    "FILTER(fn:palindrome(?label)) } LIMIT 10");
        loadInferredData(inferenceDataDir, "ssb-inf.ttl");
        //readData(inferenceDataDir, "select * where {?x rdf:type <http://class-a>}");
        readData(inferenceDataDir, "prefix rdfh: <http://lod2.eu/schemas/rdfh#> select * where {?S rdfh:s_name ?O . ?S a rdfh:lineorder}");
    }

    private static void loadDataProgramticChunking(File dataDir,String inputFile) throws RepositoryException {
        File file = new File(inputFile);
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        con.setAutoCommit(false);

        RDFParser parser = Rio.createParser(RDFFormat.forFileName(inputFile));
        parser.setRDFHandler(new ChunkCommitter(con,false));

        try
        {
            BufferedInputStream is = new BufferedInputStream(new FileInputStream(file),100*1024*1024); //100 MB
            parser.parse(is,"");
            con.commit();
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
            con.add(file, null, RDFFormat.TURTLE);
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
                            "WHERE {?fact ?dimProp ?dim . " +
                            " ?dimProp a rdfh:DimensionProperty . " +
                            " ?dim ?levelProp ?level . }",

                    "prefix rdfh: <http://lod2.eu/schemas/rdfh#> " +
                            "prefix rdfh-inst: <http://lod2.eu/schemas/rdfh-inst#> " +
                            "CONSTRUCT {?fact ?levelProp ?level} " +
                            "WHERE {?fact ?levelProp ?level . " +
                            " ?fact a rdfh-inst:lineorder . " +
                            " ?levelProp a rdfh:DimesionLevelProperty . }"
                )
            )
        );
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        try
        {
            con.add(file, null, RDFFormat.TURTLE);
            con.commit();
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
