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

import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.labpal.Random;
import ca.uqac.lif.synthia.Picker;
import ca.uqac.lif.synthia.random.RandomFloat;
import ca.uqac.lif.synthia.sequence.Interleave;
import ca.uqac.lif.synthia.sequence.MarkovChain;
import ca.uqac.lif.synthia.util.Constant;
import ca.uqac.lif.synthia.util.Freeze;
import ca.uqac.lif.synthia.util.Tick;

public class ProcessSource extends RandomSource<Object[]>
{
  protected transient Interleave<Object[]> m_interleave;
  
  protected transient int m_numInstances;
  
  public ProcessSource(Random r, int num_events, int num_instances)
  {
    super(r, num_events);
    m_numInstances = num_instances;
    m_interleave = new Interleave<Object[]>(new RandomFloat(), 1, 0.5);
    ArrayPicker ap = new ArrayPicker(new Freeze<Number>(new GlobalTick()), getMarkovChain());
    m_interleave.add(ap, 1);
  }
  
  public ProcessSource(Random r, int num_events, int num_instances, Interleave<Object[]> interleave)
  {
    super(r, num_events);
    m_numInstances = num_instances;
    m_interleave = interleave;
  }
  
  @Override
  protected Object[] getEvent()
  {
    return m_interleave.pick();
  }

  @Override
  public Object[] readEvent(String line)
  {
    String[] parts = line.split(",");
    Object[] e = new Object[2];
    e[0] = Integer.parseInt(parts[0].trim());
    e[1] = parts[1].trim();
    return e;
  }

  @Override
  public String printEvent(Object[] e)
  {
    return e[0] + "," + e[1];
  }

  @Override
  public String getFilename()
  {
    return "process.csv";
  }

  @Override
  public Processor duplicate(boolean with_state)
  {
    return new ProcessSource(m_random, m_numEvents, m_numInstances, m_interleave.duplicate(with_state));
  }
  
  protected static class ArrayPicker implements Picker<Object[]>
  {
    protected Picker<?>[] m_pickers;
    
    public ArrayPicker(Picker<?> ... pickers)
    {
      super();
      m_pickers = pickers;
    }
    
    @Override
    public Picker<Object[]> duplicate(boolean with_state)
    {
      Picker<?>[] duplicates = new Picker<?>[m_pickers.length];
      for (int i = 0; i < m_pickers.length; i++)
      {
        duplicates[i] = m_pickers[i].duplicate(with_state);
      }
      return new ArrayPicker(duplicates);
    }

    @Override
    public Object[] pick()
    {
      Object[] out = new Object[m_pickers.length];
      for (int i = 0; i < m_pickers.length; i++)
      {
        out[i] = m_pickers[i].pick();
      }
      return out;
    }

    @Override
    public void reset()
    {
      for (int i = 0; i < m_pickers.length; i++)
      {
        m_pickers[i].reset();
      }
    }
  }
  
  protected static class GlobalTick extends Tick
  {
    public GlobalTick()
    {
      super(0, 1);
    }
    
    @Override
    public GlobalTick duplicate(boolean with_state)
    {
      return this;
    }
  }
  
  protected static MarkovChain<String> getMarkovChain()
  {
    MarkovChain<String> mc = new MarkovChain<String>(new RandomFloat());
    mc.add(0, new Constant<String>(""));
    mc.add(1, new Constant<String>("a"));
    mc.add(2, new Constant<String>("b")); 
    mc.add(3, new Constant<String>("c")); 
    mc.add(4, new Constant<String>("d")); 
    mc.add(5, new Constant<String>("e"));
    // Good transitions
    mc.add(0, 1, 1).add(1, 2, 0.9).add(2, 3, 0.9).add(3, 4, 0.45).add(3, 2, 0.45);
    // Erroneous transitions
    mc.add(1, 5, 0.1).add(2, 4, 0.1).add(3, 1, 0.1);
    // Sinks
    mc.add(4, 4, 1).add(5, 5, 1);
    return mc;
  }
}
