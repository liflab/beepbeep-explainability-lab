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

import ca.uqac.lif.cep.tuples.FixedTupleBuilder;
import ca.uqac.lif.cep.tuples.Tuple;

public class CvcSource extends FileSource<Tuple>
{
  /**
   * The name of the file to read from
   */
  protected static final transient String FILENAME = "data/cvc/CCC19 - Log CSV.csv";
  
  /**
   * The builder used to create tuples out of file lines
   */
  protected transient FixedTupleBuilder m_builder;
    
  public CvcSource()
  {
    super(FILENAME);
    m_builder = null;
  }
  
  public CvcSource(int num_events)
  {
    super(FILENAME, num_events);
    m_builder = null;
  }
  
  @Override
  protected Tuple getEvent()
  {
    String line = m_scanner.nextLine();
    if (m_builder == null)
    {
      String[] attributes = line.split(",");
      m_builder = new FixedTupleBuilder(attributes);
      line = m_scanner.nextLine();
    }
    String[] parts = line.split(",");
    Tuple t = m_builder.createTuple((Object[]) parts);
    return t;
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
}
