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
import ca.uqac.lif.json.JsonNull;
import ca.uqac.lif.json.JsonNumber;
import ca.uqac.lif.json.JsonString;
import ca.uqac.lif.labpal.macro.MacroScalar;
import lineagelab.LineageLab;
import lineagelab.StreamExperiment;

public class MaxLines extends MacroScalar
{
  protected transient StreamExperiment<?> m_experiment;
  
  public MaxLines(LineageLab lab, StreamExperiment<?> exp)
  {
    super(lab, "maxLines", "The maximum number of input events that would fill 64 GB of memory");
    m_experiment = exp;
  }
  
  @Override
  public JsonElement getValue()
  {
    int mem = m_experiment.readInt(StreamExperiment.MEM_PER_EVENT);
    if (mem == 0)
    {
      return JsonNull.instance;
    }
    long lines = (64000000 / mem) * 1000;
    if (lines > 1000000)
    {
      long millions = lines / 1000000;
      return new JsonString(millions + " million");
    }
    return new JsonNumber(lines);
  }
}
