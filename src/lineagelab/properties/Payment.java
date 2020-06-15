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
package lineagelab.properties;

import ca.uqac.lif.cep.Connector;
import ca.uqac.lif.cep.EventNodeFunction;
import ca.uqac.lif.cep.EventTracker;
import ca.uqac.lif.cep.GroupProcessor;
import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.functions.ApplyFunction;
import ca.uqac.lif.cep.functions.TurnInto;
import ca.uqac.lif.cep.tmf.Fork;
import ca.uqac.lif.cep.tmf.Freeze;
import ca.uqac.lif.cep.tmf.Slice;
import ca.uqac.lif.cep.tuples.FetchAttribute;
import ca.uqac.lif.cep.util.Numbers;
import ca.uqac.lif.petitpoucet.NodeFunction;
import ca.uqac.lif.petitpoucet.ProvenanceNode;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class Payment extends Slice
{
  public static final transient String NAME = "Payment";
  
  public Payment(EventTracker t)
  {
    super(new FetchAttribute("id"), new SliceDefinition(t));
    setEventTracker(t);
  }
  
  @Override
  protected boolean produceReturn(Queue<Object[]> outputs)
  {
    Object false_slice = null;
    for (Map.Entry<Object,Object> e : m_lastValues.entrySet())
    {
      Boolean b = (Boolean) e.getValue();
      if (!b)
      {
        false_slice = e.getKey();
      }
    }
    if (false_slice == null)
    {
      outputs.add(new Object[] {true});
      if (m_eventTracker != null)
      {
        for (Processor p : m_slices.values())
        {
          associateCurrentOutputTo(p);
        }
      }
    }
    else
    {
      outputs.add(new Object[] {false});
      if (m_eventTracker != null)
      {
        Processor p = m_slices.get(false_slice);
        associateCurrentOutputTo(p);
      }
    }
    return true;
  }

  protected void associateCurrentOutputTo(Processor p)
  {
    int stream_pos = p.getOutputCount();
    EventTracker p_et = p.getEventTracker();
    if (p_et != null)
    {
      ProvenanceNode root = p_et.getProvenanceTree(p.getId(), 0, stream_pos);
      if (root != null)
      {
        List<ProvenanceNode> leaves = getLeaves(root);
        for (ProvenanceNode leaf : leaves)
        {
          NodeFunction nf = leaf.getNodeFunction();
          if (!(nf instanceof EventNodeFunction))
          {
            continue;
          }
          EventNodeFunction enf = (EventNodeFunction) nf;
          int slice_pos = enf.getStreamPosition();
          List<Integer> slice_indices = m_sliceIndices.get(p);
          m_eventTracker.associateToInput(getId(), 0, slice_indices.get(slice_pos), 0, m_outputCount);
        }
      }
    }
  }
  
  @Override
  public Payment duplicate(boolean with_state)
  {
    EventTracker t = getEventTracker();
    if (t != null)
    {
      t = t.getCopy();
    }
    return new Payment(t);
  }
  
  protected static class SliceDefinition extends GroupProcessor
  {
    public SliceDefinition(EventTracker t)
    {
      super(1, 1);
      ApplyFunction get_ts = new ApplyFunction(new FetchAttribute("timestamp"));
      Fork f = new Fork();
      Connector.connect(t, get_ts, 0, f, 0);
      Freeze fr = new Freeze();
      ApplyFunction minus = new ApplyFunction(Numbers.subtraction);
      Connector.connect(t, f, 0, minus, 0);
      Connector.connect(t, f, 1, fr, 0);
      Connector.connect(t, fr, 0, minus, 1);
      Fork f2 = new Fork();
      Connector.connect(t, minus, 0, f2, 0);
      TurnInto ti = new TurnInto(10); // 10 = max number of days
      ApplyFunction lt = new ApplyFunction(Numbers.isLessOrEqual);
      Connector.connect(t, f2, 0, lt, 0);
      Connector.connect(t, f2, 1, ti, 0);
      Connector.connect(t, ti, 0, lt, 1);
      associateInput(0, get_ts, 0);
      associateOutput(0, lt, 0);
    }
    
    @Override
    public SliceDefinition duplicate(boolean with_state)
    {
      EventTracker t = getEventTracker();
      if (t != null)
      {
        t = t.getCopy();
      }
      return new SliceDefinition(t);
    }
  }
}
