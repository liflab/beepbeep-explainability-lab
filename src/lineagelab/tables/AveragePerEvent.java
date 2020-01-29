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
package lineagelab.tables;

import ca.uqac.lif.labpal.provenance.ExperimentValue;
import ca.uqac.lif.mtnp.table.HardTable;
import ca.uqac.lif.mtnp.table.TableEntry;
import ca.uqac.lif.mtnp.table.TempTable;
import java.util.HashMap;
import java.util.Map;
import lineagelab.LineageLab;
import lineagelab.StreamExperiment;

public class AveragePerEvent extends HardTable
{
  protected Map<String,StreamExperiment<?>> m_experimentsWith;
  
  protected Map<String,StreamExperiment<?>> m_experimentsWithout;
  
  public AveragePerEvent()
  {
    super(StreamExperiment.PROPERTY, StreamExperiment.MEM_PER_EVENT);
    m_experimentsWith = new HashMap<String,StreamExperiment<?>>();
    m_experimentsWithout = new HashMap<String,StreamExperiment<?>>();
  }
  
  public void add(StreamExperiment<?> e_without, StreamExperiment<?> e_with)
  {
    String property = e_without.readString(StreamExperiment.PROPERTY);
    m_experimentsWith.put(property, e_with);
    m_experimentsWithout.put(property, e_without);
  }
  
  @Override
  public TempTable getDataTable(boolean temp)
  {
    HardTable tt = new HardTable(StreamExperiment.PROPERTY, StreamExperiment.MEM_PER_EVENT);
    for (String property : m_experimentsWith.keySet())
    {
      StreamExperiment<?> e_with = m_experimentsWith.get(property);
      StreamExperiment<?> e_without = m_experimentsWithout.get(property);
      int mem_with = e_with.readInt(StreamExperiment.MAX_MEMORY);
      int mem_without = e_without.readInt(StreamExperiment.MAX_MEMORY);
      int overhead = (mem_with - mem_without) / LineageLab.MAX_TRACE_LENGTH;
      TableEntry te = new TableEntry();
      te.put(StreamExperiment.PROPERTY, property);
      te.put(StreamExperiment.MEM_PER_EVENT, overhead);
      te.addDependency(StreamExperiment.PROPERTY, new ExperimentValue(e_with, StreamExperiment.PROPERTY));
      te.addDependency(StreamExperiment.PROPERTY, new ExperimentValue(e_without, StreamExperiment.PROPERTY));
      te.addDependency(StreamExperiment.MEM_PER_EVENT, new ExperimentValue(e_with, StreamExperiment.MAX_MEMORY));
      te.addDependency(StreamExperiment.MEM_PER_EVENT, new ExperimentValue(e_without, StreamExperiment.MAX_MEMORY));
      tt.add(te);
    }
    return tt.getDataTable(temp);
  }
}
