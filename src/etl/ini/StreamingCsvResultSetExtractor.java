package etl.ini;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


public class StreamingCsvResultSetExtractor
{
	private static char DELIMITER = '|';
	private final OutputStream os;

	/**
	** @param os the OutputStream to stream the CSV to
	**/
	public StreamingCsvResultSetExtractor(final OutputStream os)
	{
		this.os = os;
	}

	public void extractData(final Statement st, final ResultSet rs,int dificultad)
	{
		try (PrintWriter pw = new PrintWriter(os, true))
		{
			String linea;
			final ResultSetMetaData rsmd = rs.getMetaData();
			final int columnCount = rsmd.getColumnCount();
			writeHeader(rsmd, columnCount, pw);
			st.setFetchSize(50);
			while (rs.next())
			{
				for (int i = 1; i <= columnCount; i++)
				{
					final Object value = rs.getObject(i);
					if (dificultad==0)
					{
						pw.write(value == null ? "" : value.toString());
					}
					else
					{
						linea=value == null ? "" : value.toString();
						linea=linea.contains("|")?linea.replace("|"," "):linea;
						linea=linea.contains("\"")?linea.replace("\"",""):linea;
						linea=linea.contains("\r")?linea.replace("\r"," "):linea;
						linea=linea.contains("\n")?linea.replace("\n"," "):linea;
						pw.write(linea);
					}
					if (i != columnCount)
					{
						pw.append(DELIMITER);
						
					}
				}
				pw.println();
			}
			pw.flush();
			pw.close();
		}
	
		catch (final SQLException e)
		{
			System.out.println(e.getMessage());
			throw new RuntimeException(e);
		}
		return;
	}

	private static void writeHeader(final ResultSetMetaData rsmd,final int columnCount, final PrintWriter pw)
	{
		for (int i = 1; i <= columnCount; i++)
		{
			try
			{
				pw.write(rsmd.getColumnName(i));
			}
			catch (SQLException e)
			{
				System.out.println(e.getMessage());
				return;
			}
			if (i != columnCount)
			{
				pw.append(DELIMITER);
			}
		}
		pw.println();
	}
}