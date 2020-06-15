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
import ca.uqac.lif.cep.tuples.FetchAttribute;
import ca.uqac.lif.cep.util.Equals;
import ca.uqac.lif.petitpoucet.NodeFunction;
import ca.uqac.lif.petitpoucet.ProvenanceNode;
import java.util.List;

public class CvcProcedure extends Slice
{
  public static final transient String NAME = "CVC Procedure";
  
  public CvcProcedure(EventTracker t)
  {
    super(new FetchAttribute("CASEID"), new SliceDefinition(t));
    setEventTracker(t);
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
      ApplyFunction get_action = new ApplyFunction(new FetchAttribute("ACTIVITY"));
      MooreMachine mm = new MooreMachine(1, 1);
      {
        mm.addTransition(0, new FunctionTransition(getEqualTo("Prepare Implements"), 1));
        mm.addTransition(1, new FunctionTransition(getEqualTo("Hand washing"), 2));
        mm.addTransition(2, new FunctionTransition(getEqualTo("Get in sterile clothes"), 3));
        mm.addTransition(3, new FunctionTransition(getEqualTo("Clean puncture area"), 4));
        mm.addTransition(4, new FunctionTransition(getEqualTo("Drap puncture area"), 5));
        mm.addTransition(5, new FunctionTransition(getEqualTo("Ultrasound configuration"), 6));
        mm.addTransition(6, new FunctionTransition(getEqualTo("Gel in probe"), 7));
        mm.addTransition(7, new FunctionTransition(getEqualTo("Cover probe"), 8));
        mm.addTransition(8, new FunctionTransition(getEqualTo("Put sterile gel"), 9));
        mm.addTransition(9, new FunctionTransition(getEqualTo("Position probe"), 10));
        mm.addTransition(10, new FunctionTransition(getEqualTo("Position patient"), 11));
        mm.addTransition(11, new FunctionTransition(getEqualTo("Anatomic identification"), 14));
        mm.addTransition(12, new FunctionTransition(getEqualTo("Doppler identification"), 14));
        mm.addTransition(13, new FunctionTransition(getEqualTo("Compression identification"), 14));
        mm.addTransition(14, new FunctionTransition(getEqualTo("Anesthetize"), 15));
        mm.addTransition(15, new FunctionTransition(getEqualTo("Puncture"), 16));
        mm.addTransition(16, new FunctionTransition(getEqualTo("Blood return"), 17));
        mm.addTransition(17, new FunctionTransition(getEqualTo("Puncture"), 16));
        mm.addTransition(17, new FunctionTransition(getEqualTo("Drop probe"), 18));
        mm.addTransition(18, new FunctionTransition(getEqualTo("Remove syringe"), 19));
        mm.addTransition(19, new FunctionTransition(getEqualTo("Guidewire install"), 20));
        mm.addTransition(20, new FunctionTransition(getEqualTo("Remove trocar"), 21));
        mm.addTransition(21, new FunctionTransition(getEqualTo("Check wire in long axis"), 22));
        mm.addTransition(21, new FunctionTransition(getEqualTo("Check wire in short axis"), 22));
        mm.addTransition(22, new FunctionTransition(getEqualTo("Wire in good position"), 23));
        mm.addTransition(23, new FunctionTransition(getEqualTo("Widen pathway"), 24));
        mm.addTransition(23, new FunctionTransition(getEqualTo("Puncture"), 16));
        mm.addTransition(24, new FunctionTransition(getEqualTo("Advance catheter"), 25));
        mm.addTransition(25, new FunctionTransition(getEqualTo("Remove guidewire"), 26));
        mm.addTransition(26, new FunctionTransition(getEqualTo("Check flow and reflow"), 27));
        mm.addTransition(27, new FunctionTransition(getEqualTo("Check catheter position"), 28));
        for (int i = 0; i < 28; i++)
        {
          mm.addTransition(i, new TransitionOtherwise(29));
        }
        mm.addTransition(28, new TransitionOtherwise(28));
        mm.addTransition(29, new TransitionOtherwise(29));
        for (int i = 0; i <= 28; i++)
        {
          mm.addSymbol(i, TRUE);
        }
        mm.addSymbol(29, FALSE);
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
  public CvcProcedure duplicate(boolean with_state)
  {
    EventTracker t = getEventTracker();
    if (t != null)
    {
      t = t.getCopy();
    }
    return new CvcProcedure(t);
  }
}
