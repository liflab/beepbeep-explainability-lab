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
package lineagelab.macros;

import ca.uqac.lif.json.JsonElement;
import ca.uqac.lif.json.JsonString;
import ca.uqac.lif.labpal.Formatter;
import ca.uqac.lif.labpal.macro.MacroMap;
import ca.uqac.lif.labpal.table.VersusTable;
import ca.uqac.lif.mtnp.table.PrimitiveValue;
import ca.uqac.lif.mtnp.table.TableEntry;
import ca.uqac.lif.mtnp.table.TempTable;
import java.util.Map;
import lineagelab.LineageLab;

public class OverheadMacro extends MacroMap
{
  protected static final transient String MAX_OH_MEM = "maxOhMemory";
  
  protected static final transient String MAX_OH_TIME = "maxOhThroughput";
  
  protected VersusTable m_memTable;
  
  protected VersusTable m_timeTable;
  
  public OverheadMacro(LineageLab lab, VersusTable mem_table, VersusTable time_table)
  {
    super(lab);
    add(MAX_OH_MEM, "Maximum overhead in terms of memory");
    add(MAX_OH_TIME, "Maximum overhead in terms of throughput");
    m_memTable = mem_table;
    m_timeTable = time_table;
  }
  
  @Override
  public void computeValues(Map<String,JsonElement> map)
  {
    map.put(MAX_OH_MEM, new JsonString(getMaxOverhead(m_memTable, LineageLab.NO_TRACKER, LineageLab.WITH_TRACKER)));
    map.put(MAX_OH_TIME, new JsonString(getMaxOverhead(m_timeTable, LineageLab.WITH_TRACKER, LineageLab.NO_TRACKER)));
  }
  
  protected String getMaxOverhead(VersusTable table, String n1, String n2)
  {
    float oh = 0;
    TempTable tt = table.getDataTable();
    int max_row = -1, cur_row = 0;
    for (TableEntry te : tt.getEntries())
    {
      PrimitiveValue pv1 = te.get(n1);
      PrimitiveValue pv2 = te.get(n2);
      if (pv1 == null || pv2 == null)
      {
        continue;
      }
      float f1 = pv1.numberValue().floatValue();
      float f2 = pv2.numberValue().floatValue();
      float oh_e = Formatter.divide(f2, f1);
      if (oh_e > oh)
      {
        max_row = cur_row;
        oh = oh_e;
      }
      cur_row++;
    }
    // TODO: add lineage between output macro and max_row
    return Float.toString(Formatter.sigDig(oh));
  }
}
