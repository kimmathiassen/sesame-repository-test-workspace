/**
 * Created by kim on 2/20/14.
 */
import org.openrdf.model.*;
import org.openrdf.repository.*;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFParseException;
import org.openrdf.OpenRDFException;
import org.openrdf.rio.RDFFormat;
import java.io.File;

import org.openrdf.sail.nativerdf.NativeStore;
import java.io.IOException;
import java.util.List;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;

public class Main {

    private static File testDataDir = new File("dbs/test/");
    private static File agriBusiDataDir = new File("dbs/agri-busi/");
    private static String indexes = "spoc,posc,cosp,cspo,cpos";

    public static void main(String args[]) throws RepositoryException {
        loadData(testDataDir,"test.ttl");
        readData(testDataDir);
    }

    private static void loadData(File dataDir,String inputFile) throws RepositoryException {
        File file = new File(inputFile);
        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();
        try
        {
            con.add(file,"" , RDFFormat.TURTLE);
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

    private static void readData(File dataDir) throws RepositoryException {
        org.openrdf.repository.Repository repo = null;
        repo = new SailRepository(new NativeStore(dataDir, indexes));

        repo.initialize();

        RepositoryConnection con = repo.getConnection();
        try
        {
            String queryString = "SELECT ?x ?y WHERE { ?x ?p ?y } ";
            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            TupleQueryResult result = tupleQuery.evaluate();

            List<String> bindingNames = result.getBindingNames();
            int i = 0;
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                Value firstValue = bindingSet.getValue(bindingNames.get(0));
                Value secondValue = bindingSet.getValue(bindingNames.get(1));

                System.out.println(firstValue + " " + secondValue);
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
