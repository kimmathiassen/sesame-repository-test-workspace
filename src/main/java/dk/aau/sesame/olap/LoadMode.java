package dk.aau.sesame.olap;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.*;
import org.openrdf.sail.nativerdf.NativeStore;

import javax.xml.crypto.Data;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by alex on 3/26/14.
 */
public class LoadMode implements Mode{

    private boolean commit;
    private long chunkSize;
    private String indexes;

    public LoadMode(boolean commit, long chunkSize, String indexes) {
        this.commit = commit;
        this.chunkSize = chunkSize;
        this.indexes = indexes;
    }

    public void handle(String dataDirName,String inputFileName) throws Exception
    {
        File inputFile = new File(inputFileName);
        File dataDir = new File(dataDirName);

        org.openrdf.repository.Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
        repo.initialize();
        RepositoryConnection con = repo.getConnection();

        try
        {
            con.setAutoCommit(false); // This seems to be necessary for speeding up windows
            if(inputFile.isDirectory())
            {
                loadDataDirectory(con,inputFile);
            }
            else
            {
                if(inputFile.isFile())
                {
                    loadDataProgramticChunking(con,inputFile);
                }
                else
                {
                    throw new IllegalArgumentException("Invalid file specified for load data");
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            con.close();
            repo.shutDown();
        }
    }

    private void loadDataProgramticChunking(RepositoryConnection con,File inputFile) throws RDFParseException, IOException, RDFHandlerException
    {
        RDFParser parser = Rio.createParser(RDFFormat.forFileName(inputFile.getName()));
        parser.setRDFHandler(new ChunkCommitter(con,commit,chunkSize));

        BufferedInputStream is = new BufferedInputStream(new FileInputStream(inputFile),10*1024*1024); //10 MB
        parser.parse(is,"");
    }

    private void loadDataDirectory(RepositoryConnection con,File inputFileDir) throws RepositoryException, IOException, RDFParseException
    {
        int i = 0;
        File[] files = inputFileDir.listFiles();
        long start = System.nanoTime();
        con.begin();
        for (File file: files)
        {
            con.add(file, null, RDFFormat.NTRIPLES);
            ++i;
            System.out.println("Files processed: " + i + "/" + files.length + ". Time: " + (System.nanoTime()-start)/1000000 + " ms");
            if(commit)
            {
                con.commit();
                con.begin();
            }
        }
        con.commit();
    }
}
