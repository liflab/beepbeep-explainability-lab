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
import ca.uqac.lif.cep.fsm.FunctionTransition;
import ca.uqac.lif.cep.fsm.MooreMachine;
import ca.uqac.lif.cep.fsm.TransitionOtherwise;
import ca.uqac.lif.cep.functions.ApplyFunction;
import ca.uqac.lif.cep.functions.Constant;
import ca.uqac.lif.cep.functions.Function;
import ca.uqac.lif.cep.functions.FunctionTree;
import ca.uqac.lif.cep.functions.StreamVariable;
import ca.uqac.lif.cep.tmf.Slice;
import ca.uqac.lif.cep.util.Equals;
import ca.uqac.lif.cep.util.NthElement;
import ca.uqac.lif.petitpoucet.NodeFunction;
import ca.uqac.lif.petitpoucet.ProvenanceNode;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class ProcessLifecycle extends Slice
{
  public static final transient String NAME = "Process lifecycle";

  public ProcessLifecycle(EventTracker t)
  {
    super(new NthElement(0), new SliceDefinition(t));
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

  protected static class SliceDefinition extends GroupProcessor
  {
    protected static final Constant TRUE = new Constant(true);
    protected static final Constant FALSE = new Constant(false);

    public SliceDefinition(EventTracker t)
    {
      super(1, 1);
      ApplyFunction get_action = new ApplyFunction(new NthElement(1));
      MooreMachine mm = new MooreMachine(1, 1);
      {
        mm.addSymbol(0, TRUE);
        mm.addSymbol(1, TRUE);
        mm.addSymbol(2, TRUE);
        mm.addSymbol(3, TRUE);
        mm.addSymbol(4, FALSE);
        mm.addTransition(0, new FunctionTransition(getEqualTo("a"), 1));
        mm.addTransition(1, new FunctionTransition(getEqualTo("b"), 2));
        mm.addTransition(2, new FunctionTransition(getEqualTo("c"), 1));
        mm.addTransition(2, new FunctionTransition(getEqualTo("d"), 3));
        mm.addTransition(0, new TransitionOtherwise(4));
        mm.addTransition(1, new TransitionOtherwise(4));
        mm.addTransition(2, new TransitionOtherwise(4));
        mm.addTransition(3, new TransitionOtherwise(4));
        mm.addTransition(4, new TransitionOtherwise(4));
      }
      Connector.connect(t, get_action, mm);
      addProcessors(get_action, mm);
      associateInput(0, get_action, 0);
      associateOutput(0, mm, 0);
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

    protected static Function getEqualTo(String s)
    {
      return new FunctionTree(Equals.instance, StreamVariable.X, new Constant(s));
    }
  }

  @Override
  public ProcessLifecycle duplicate(boolean with_state)
  {
    EventTracker t = getEventTracker();
    if (t != null)
    {
      t = t.getCopy();
    }
    return new ProcessLifecycle(t);
  }
}
