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

import ca.uqac.lif.labpal.Random;

/**
 * A source of randomly generated numbers.
 */
public class RandomNumberSource extends RandomSource<Float>
{
  /**
   * Creates a new random number source.
   * @param r The random number generator used to generate the numbers
   * @param num_events The number of events to produce
   */
  public RandomNumberSource(Random r, int num_events)
  {
    super(r, num_events);
  }
  
  @Override
  protected Float getEvent()
  {
    return m_random.nextFloat();
  }

  @Override
  public RandomNumberSource duplicate(boolean with_state)
  {
    return new RandomNumberSource(m_random, m_numEvents);
  }

  @Override
  public Float readEvent(String line)
  {
    return Float.parseFloat(line.trim());
  }

  @Override
  public String printEvent(Float e)
  {
    return e.toString();
  }
  
  @Override
  public String getFilename()
  {
    return "numbers-" + m_numEvents + ".csv";
  }
}
