package eu.clarussecure.proxy.protocol.plugins.pgsql.message.sql;

import java.util.ArrayList;
import java.util.List;

import eu.clarussecure.proxy.spi.CString;
import net.sf.jsqlparser.statement.Statement;

public class SqlSession {
    public static class BufferedStatement {
        private final CString original;
        private final boolean toProcess;
        private Statement stmt;
        private CString modified;

        public BufferedStatement(CString statement) {
            this(statement, false);
        }

        public BufferedStatement(CString statement, boolean toProcess) {
            this.original = statement;
            this.toProcess = toProcess;
            this.modified = null;
        }

        public CString getOriginal() {
            return original;
        }

        public boolean isToProcess() {
            return toProcess;
        }

        public Statement getStmt() {
            return stmt;
        }

        public void setStmt(Statement stmt) {
            this.stmt = stmt;
        }

        public CString getModified() {
            return modified;
        }

        public void setModified(CString modified) {
            this.modified = modified;
            this.stmt = null;
        }

        public CString getResult() {
            return toProcess ? modified : original;
        }

    }

    private CString databaseName;
    private boolean inTransaction;
    private boolean inDatasetCreation;
    private TransferMode transferMode;
    private List<BufferedStatement> bufferedStatements;
    private int commandResultsToIgnore;
    private int readyForQueryToIgnore;

    public CString getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(CString databaseName) {
        if (this.databaseName != null) {
            // Release internal buffer of database name
            this.databaseName.release();
        }
        this.databaseName = databaseName;
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    public void setInTransaction(boolean inTransaction) {
        this.inTransaction = inTransaction;
    }

    public boolean isInDatasetCreation() {
        return inDatasetCreation;
    }

    public void setInDatasetCreation(boolean inDatasetCreation) {
        this.inDatasetCreation = inDatasetCreation;
    }

    public TransferMode getTransferMode() {
        return transferMode;
    }

    public void setTransferMode(TransferMode transferMode) {
        this.transferMode = transferMode;
    }

    public List<BufferedStatement> getBufferedStatements() {
        if (bufferedStatements == null) {
            bufferedStatements = new ArrayList<>();
        }
        return bufferedStatements;
    }

    public void setBufferedStatements(List<BufferedStatement> bufferedStatements) {
        this.bufferedStatements = bufferedStatements;
    }

    public boolean addBufferedStatements(CString statement, boolean toProcess) {
        // Retain internal buffer of statement
        if (statement.isBuffered()) {
            statement.retain();
        }
        return getBufferedStatements().add(new BufferedStatement(statement, toProcess));
    }

    public void resetBufferedStatements() {
        bufferedStatements.clear();
    }

    public int getCommandResultsToIgnore() {
        return commandResultsToIgnore;
    }

    public void resetCommandResultsToIgnore() {
        commandResultsToIgnore = 0;
    }

    public void incrementeCommandResultsToIgnore() {
        commandResultsToIgnore ++;
    }

    public void decrementeCommandResultsToIgnore() {
        commandResultsToIgnore --;
    }

    public int getReadyForQueryToIgnore() {
        return readyForQueryToIgnore;
    }

    public void resetReadyForQueryToIgnore() {
        readyForQueryToIgnore = 0;
    }

    public void incrementeReadyForQueryToIgnore() {
        readyForQueryToIgnore ++;
    }

    public void decrementeReadyForQueryToIgnore() {
        readyForQueryToIgnore --;
    }

    public void release() {
        if (databaseName != null) {
            databaseName.release();
        }
        if (bufferedStatements != null) {
            for (BufferedStatement bufferedStatement : bufferedStatements) {
                if (bufferedStatement.getOriginal() != null) {
                    bufferedStatement.getOriginal().release();
                }
                if (bufferedStatement.getModified() != null) {
                    bufferedStatement.getModified().release();
                }
            }
        }
    }

}
