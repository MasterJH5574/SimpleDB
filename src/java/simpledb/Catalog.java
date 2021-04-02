package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * The Catalog keeps track of all available tables in the database and their associated schemas. For
 * now, this is a stub catalog that must be populated with tables by a user program before it can be
 * used -- eventually, this should be converted to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
  /**
   * A class representing all tables
   */
  private static final class Table {
    /**
     * The DbFile of the table
     */
    public final DbFile file;
    /**
     * The name of the table
     */
    public final String name;
    /**
     * The name of the primary key field of the table
     */
    public final String pkeyField;

    public Table(DbFile file, String name, String pkeyField) {
      this.file = file;
      this.name = name;
      this.pkeyField = pkeyField;
    }
  }

  /**
   * A mapping from table ids to tables
   */
  private final Map<Integer, Table> tableMap;
  /**
   * A mapping from table names to table ids
   */
  private final Map<String, List<Integer>> nameMap;

  /**
   * Constructor. Creates a new, empty catalog.
   */
  public Catalog() {
    tableMap = new HashMap<>();
    nameMap = new HashMap<>();
  }

  /**
   * Add a new table to the catalog. This table's contents are stored in the specified DbFile.
   *
   * @param file      the contents of the table to add;  file.getId() is the identifier of this
   *                  file/tupleDesc param for the calls getTupleDesc and getFile
   * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
   *                  conflict exists, use the last added table as the table for a given name.
   * @param pkeyField the name of the primary key field
   */
  public void addTable(DbFile file, String name, String pkeyField) {
    tableMap.put(file.getId(), new Table(file, name, pkeyField));
    List<Integer> ids = nameMap.get(name);
    if (ids == null) {
      ids = new ArrayList<>();
      ids.add(file.getId());
      nameMap.put(name, ids);
    } else {
      ids.add(file.getId());
    }
  }

  public void addTable(DbFile file, String name) {
    addTable(file, name, "");
  }

  /**
   * Add a new table to the catalog. This table has tuples formatted using the specified TupleDesc
   * and its contents are stored in the specified DbFile.
   *
   * @param file the contents of the table to add;  file.getId() is the identifier of this
   *             file/tupleDesc param for the calls getTupleDesc and getFile
   */
  public void addTable(DbFile file) {
    addTable(file, (UUID.randomUUID()).toString());
  }

  /**
   * Return the id of the table with a specified name
   *
   * @throws NoSuchElementException if the table doesn't exist
   */
  public int getTableId(String name) throws NoSuchElementException {
    List<Integer> ids = nameMap.get(name);
    if (ids == null) {
      throw new NoSuchElementException();
    }
    assert !ids.isEmpty();
    return ids.get(ids.size() - 1);
  }

  /**
   * A helper method which returns the table corresponding to a tableid
   *
   * @param tableid The id of the table
   * @throws NoSuchElementException if the table doesn't exist
   */
  private Table getTable(int tableid) throws NoSuchElementException {
    Table table = tableMap.get(tableid);
    if (table == null) {
      throw new NoSuchElementException();
    }
    return table;
  }

  /**
   * Returns the tuple descriptor (schema) of the specified table
   *
   * @param tableid The id of the table, as specified by the DbFile.getId() function passed to
   *                addTable
   * @throws NoSuchElementException if the table doesn't exist
   */
  public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    return this.getTable(tableid).file.getTupleDesc();
  }

  /**
   * Returns the DbFile that can be used to read the contents of the specified table
   *
   * @param tableid The id of the table, as specified by the DbFile.getId() function passed to
   *                addTable
   * @throws NoSuchElementException if the table doesn't exist
   */
  public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    return this.getTable(tableid).file;
  }

  /**
   * Returns the name of the primary key field of the specified table
   *
   * @param tableid The id of the table, as specified by the DbFile.getId() function passed to
   *                addTable
   * @throws NoSuchElementException if the table doesn't exist
   */
  public String getPrimaryKey(int tableid) throws NoSuchElementException {
    return this.getTable(tableid).pkeyField;
  }

  /**
   * Return an iterator of the table ids of the tables
   */
  public Iterator<Integer> tableIdIterator() {
    return this.tableMap.keySet().iterator();
  }

  /**
   * Returns the name of the table specified by tableid
   *
   * @param id The id of the table
   * @throws NoSuchElementException if the table doesn't exist
   */
  public String getTableName(int id) throws NoSuchElementException {
    return getTable(id).name;
  }

  /**
   * Delete all tables from the catalog
   */
  public void clear() {
    tableMap.clear();
    nameMap.clear();
  }

  /**
   * Reads the schema from a file and creates the appropriate tables in the database.
   *
   * @param catalogFile The name of the catalog file
   */
  public void loadSchema(String catalogFile) {
    String line = "";
    String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
    try {
      BufferedReader br = new BufferedReader(new FileReader(catalogFile));

      while ((line = br.readLine()) != null) {
        //assume line is of the format name (field type, field type, ...)
        String name = line.substring(0, line.indexOf("(")).trim();
        //System.out.println("TABLE NAME: " + name);
        String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
        String[] els = fields.split(",");
        ArrayList<String> names = new ArrayList<>();
        ArrayList<Type> types = new ArrayList<>();
        String primaryKey = "";
        for (String e : els) {
          String[] els2 = e.trim().split(" ");
          names.add(els2[0].trim());
          if (els2[1].trim().equalsIgnoreCase("int")) {
            types.add(Type.INT_TYPE);
          } else if (els2[1].trim().equalsIgnoreCase("string")) {
            types.add(Type.STRING_TYPE);
          } else {
            System.out.println("Unknown type " + els2[1]);
            System.exit(0);
          }
          if (els2.length == 3) {
            if (els2[2].trim().equals("pk")) {
              primaryKey = els2[0].trim();
            } else {
              System.out.println("Unknown annotation " + els2[2]);
              System.exit(0);
            }
          }
        }
        Type[] typeAr = types.toArray(new Type[0]);
        String[] namesAr = names.toArray(new String[0]);
        TupleDesc t = new TupleDesc(typeAr, namesAr);
        HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
        addTable(tabHf, name, primaryKey);
        System.out.println("Added table : " + name + " with schema " + t);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(0);
    } catch (IndexOutOfBoundsException e) {
      System.out.println("Invalid catalog entry : " + line);
      System.exit(0);
    }
  }
}

