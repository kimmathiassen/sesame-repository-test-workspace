package dk.aau.sesame.olap;

import org.openrdf.OpenRDFException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;

import java.io.File;
import java.util.List;
import java.util.Scanner;

/**
 * Created by alex on 3/26/14.
 */
public class QueryMode implements Mode {
    private String indexes;

    public QueryMode(String indexes) {
        this.indexes = indexes;
    }

    @Override
    public void handle(String dataDir, String arg) throws Exception {
        File queryFile = new File(arg);
        if(queryFile.exists())
        {
            if(queryFile.isDirectory())
            {
                for(File file : queryFile.listFiles())
                {
                    handle(dataDir, file.getAbsolutePath());
                }
            }
            else if(queryFile.canRead())
            {
                arg = new Scanner(queryFile).useDelimiter("\\Z").next();
                handle(dataDir, arg);
            }
            else
            {
                throw new IllegalArgumentException("Unable to read file: '" + arg + "' for query");
            }
            return;
        }

        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(new File(dataDir), indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        try
        {
            TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, arg);

            TupleQueryResult result = tupleQuery.evaluate();

            List<String> bindingNames = result.getBindingNames();
            int i = 0;
            System.out.println("@prefix lod2-inst: <http://lod2.eu/schemas/rdfh-inst>");
            System.out.println("@prefix lod2: <http://lod2.eu/schemas/rdfh>");
            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                if(i < 100)
                {
                    for (String name : bindingNames)
                    {
                        System.out.print(removeNamespace(bindingSet.getValue(name) + " "));
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
    private String removeNamespace(String binding)
    {
        String result;
        if (binding.contains("http://lod2.eu/schemas/rdfh-inst"))
        {
            result = binding.replace("http://lod2.eu/schemas/rdfh-inst", "lod2-inst:");
        }
        else
        {
            result = binding.replace("http://lod2.eu/schemas/rdfh", "lod2:");
        }
        return result;
    }
}
