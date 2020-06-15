/*
    A benchmark for data lineage in BeepBeep 3
    Copyright (C) 2017-2020 Laboratoire d'informatique formelle

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package lineagelab.source;

import ca.uqac.lif.cep.ProcessorException;
import ca.uqac.lif.cep.Pullable;
import ca.uqac.lif.cep.tuples.FixedTupleBuilder;
import ca.uqac.lif.cep.tuples.Tuple;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XesSource extends FileSource<Tuple>
{
  /**
   * The name of the file to read from
   */
  protected static final transient String FILENAME = "data/payment/RequestForPayment.xes";

  /**
   * The builder used to create tuples out of file lines
   */
  protected transient FixedTupleBuilder m_builder = new FixedTupleBuilder("id", "action", "timestamp");

  /**
   * The matcher to read the line containing the case ID
   */
  protected static transient Pattern s_caseIdPattern = Pattern.compile("\\s*<string key=\"Rfp_id\" value=\"(.*?)\"/>");

  /**
   * The matcher to read the line containing the action name
   */
  protected static transient Pattern s_actionPattern = Pattern.compile("\\s*<string key=\"concept:name\" value=\"(.*?)\"/>");

  /**
   * The matcher to read the line containing the timestamp of an action
   */
  protected static transient Pattern s_datePattern = Pattern.compile("\\s*<date key=\"time:timestamp\" value=\"(.*?)T.*?\"/>");
  
  /**
   * The date format used in the timestamps
   */
  protected static transient DateFormat s_dateFormat =  new SimpleDateFormat("yyyy-MM-dd");

  /**
   * The ID of the case currently being read
   */
  protected String m_caseId;

  public XesSource()
  {
    super(FILENAME);
    m_caseId = null;
  }

  public XesSource(int num_events)
  {
    super(FILENAME, num_events);
    m_caseId = null;
  }

  @Override
  protected Tuple getEvent()
  {
    String action = "", time = "";
    while (m_scanner.hasNextLine() && (action.isEmpty() || time.isEmpty()))
    {
      String line = m_scanner.nextLine();
      {
        Matcher mat = s_caseIdPattern.matcher(line);
        if (mat.matches())
        {
          m_caseId = mat.group(1);
          continue;
        }
      }
      {
        Matcher mat = s_actionPattern.matcher(line);
        if (mat.matches())
        {
          action = mat.group(1);
          continue;
        }
      }
      {
        Matcher mat = s_datePattern.matcher(line);
        if (mat.matches())
        {
          time = mat.group(1);
          continue;
        }
      }
    }
    if (action.isEmpty() || time.isEmpty() || m_caseId == null)
    {
      return null;
    }
    
    Tuple t;
    try
    {
      t = m_builder.createTuple(m_caseId, action, s_dateFormat.parse(time).getTime() / (24 * 60 * 60 * 1000));
      return t;
    }
    catch (ParseException e)
    {
      throw new ProcessorException(e);
    }
  }

  @Override
  public Tuple readEvent(String line)
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String printEvent(Tuple e)
  {
    // TODO Auto-generated method stub
    return null;
  }
  
  public static void main(String[] args)
  {
    XesSource s = new XesSource();
    Pullable p = s.getPullableOutput();
    while (p.hasNext())
    {
      System.out.println(p.next());
    }
  }
}
