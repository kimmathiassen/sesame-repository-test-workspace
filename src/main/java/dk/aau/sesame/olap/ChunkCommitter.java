package dk.aau.sesame.olap;

import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.util.RDFInserter;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * Created by alex on 3/13/14.
 */
public class ChunkCommitter implements RDFHandler
{
    private RDFInserter inserter;
    private RepositoryConnection conn;
    private long start;
    private boolean commit;
    private long chunkSize;

    private long count = 0L;
    private long lastTime;


    public ChunkCommitter(RepositoryConnection conn, boolean commit, long chunkSize) {
        inserter = new RDFInserter(conn);
        this.conn = conn;
        this.commit = commit;
        this.chunkSize = chunkSize;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        inserter.startRDF();
        try {
            start = System.nanoTime();
            lastTime = start;
            conn.begin();
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        inserter.endRDF();
        try {
            conn.commit();
            long time = System.nanoTime();
            System.out.println("Triples loaded: " + count + ". Time: " + (time-start)/1000000 + " ms. Load speed avg: " + count*1000000000/(time-start) + " stm/s" +
                    " - commit");
        } catch (RepositoryException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleNamespace(String prefix, String uri)
            throws RDFHandlerException {
        inserter.handleNamespace(prefix, uri);
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        inserter.handleStatement(st);
        count++;
        // do an intermittent commit whenever the number of triples
        // has reached a multiple of the chunk size
        if (chunkSize > 0 && count % chunkSize == 0) {
            try {
                if(commit)
                {
                    conn.commit();
                    conn.begin();
                }
                long time = System.nanoTime();
                System.out.println("Triples loaded: " + count + ". Time: " + (time-start)/1000000 + " ms. Load speed avg: " +
                        count*1000000000/(time-start) + " stm/s. Load speed cur: " + chunkSize*1000000000/(time-lastTime) + " stm/s" +
                        (commit ? " - commit" : ""));
                lastTime = time;
            } catch (RepositoryException e) {
                throw new RDFHandlerException(e);
            }
        }
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        inserter.handleComment(comment);
    }
}
