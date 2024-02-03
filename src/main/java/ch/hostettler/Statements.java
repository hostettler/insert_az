package ch.hostettler;

import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Statements {

    private static final short NB_STRING_FIELDS = 32;
    private static final short NB_INTEGER_FIELDS = 32;
    private static final short NB_DOUBLE_FIELDS = 32;
    private static final short VARCHAR_LENGTH = 64;
    private static final Rnd rnd = new Rnd();
    private static final String TABLE_NAME = "RESULTS";
    private static final String TEMP_CHAR_QUALIFIER = "##";

    private static final List<Pair<JDBCType, Short>> structure = new ArrayList<>(
            List.of(Pair.create(JDBCType.VARCHAR, NB_STRING_FIELDS), Pair.create(JDBCType.INTEGER, NB_INTEGER_FIELDS), Pair.create(JDBCType.DECIMAL, NB_DOUBLE_FIELDS)));

    public static String getCreateTableStatement(boolean temp, boolean columnStoreIndex) {
        StringBuffer buffer = new StringBuffer();

        String tempChar = temp ? TEMP_CHAR_QUALIFIER : "";
        buffer.append(String.format("CREATE TABLE %s%s (", tempChar, TABLE_NAME));

        int i = 0;
        buffer.append(String.format(" row_id_%d int NOT NULL PRIMARY KEY, ", i++));
        for (Pair<JDBCType, Short> p : structure) {
            switch (p.getE1()) {
            case VARCHAR:
                for (int k = 0; k < p.getE2(); k++) {
                    buffer.append(String.format(" var_%d varchar(%d), ", i++, VARCHAR_LENGTH));
                }
                break;
            case INTEGER:
                for (int k = 0; k < p.getE2(); k++) {
                    buffer.append(String.format(" int_%d integer, ", i++));
                }
                break;
            case DECIMAL:
                for (int k = 0; k < p.getE2(); k++) {
                    buffer.append(String.format(" dec_%d DECIMAL, ", i++));
                }
                break;
            default:
            }

        }
        buffer.append(String.format("pad_%d integer", i));
        buffer.append(columnStoreIndex ? String.format(" , INDEX CCIDX CLUSTERED COLUMNSTORE") : "");
        buffer.append(String.format(")"));
       
        
        return buffer.toString();
    }

    public static String getDropTableStatement(boolean temp) {
        String tempChar = temp ? TEMP_CHAR_QUALIFIER : "";        
        return String.format("DROP TABLE IF EXISTS %s%s", tempChar, TABLE_NAME);
    }

    public static String getInsertStatement(boolean temp) {
        StringBuffer buffer = new StringBuffer();
        String tempChar = temp ? TEMP_CHAR_QUALIFIER: "";
        buffer.append(String.format("INSERT INTO %s%s (", tempChar, TABLE_NAME));

        int i = 0;
        buffer.append(String.format("row_id_%d, ", i++));
        for (Pair<JDBCType, Short> p : structure) {
            switch (p.getE1()) {
            case VARCHAR:
                for (int k = 0; k < p.getE2(); k++) {
                    buffer.append(String.format(" var_%d, ", i++));
                }
                break;
            case INTEGER:
                for (int k = 0; k < p.getE2(); k++) {
                    buffer.append(String.format(" int_%d, ", i++));
                }
                break;
            case DECIMAL:
                for (int k = 0; k < p.getE2(); k++) {
                    buffer.append(String.format(" dec_%d, ", i++));
                }
                break;
            default:
            }
        }
        buffer.append(String.format("pad_%d", i));
        buffer.append(String.format(") VALUES (", TABLE_NAME));
        for (int j = 0; j < NB_STRING_FIELDS + NB_INTEGER_FIELDS + NB_DOUBLE_FIELDS + 1; j++)
            buffer.append("?, ");
        buffer.append("?)");
        return buffer.toString();
    }

    public static void fillPreparedStatement(PreparedStatement statement, long rowId) throws SQLException {
        int i = 1;
        statement.setLong(i++, rowId);
        for (Pair<JDBCType, Short> p : structure) {
            switch (p.getE1()) {
            case VARCHAR:
                for (int k = 0; k < p.getE2(); k++) {
                    statement.setString(i++, rnd.nextString(32));
                }
                break;
            case INTEGER:
                for (int k = 0; k < p.getE2(); k++) {
                    statement.setInt(i++, rnd.nextInt());
                }
                break;
            case DECIMAL:
                for (int k = 0; k < p.getE2(); k++) {
                    statement.setDouble(i++, rnd.nextDouble());
                }
                break;
            default:
            }
        }
        statement.setInt(i++, 0);
    }

    static class Pair<E1, E2> {
        private E1 e1;
        private E2 e2;

        private Pair(E1 e1, E2 e2) {
            this.e1 = e1;
            this.e2 = e2;
        }

        static <E1, E2> Pair<E1, E2> create(E1 e1, E2 e2) {
            return new Pair<>(e1, e2);
        }

        E1 getE1() {
            return e1;
        }

        E2 getE2() {
            return e2;
        }
    }
}
