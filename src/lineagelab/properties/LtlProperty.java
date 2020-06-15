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
import ca.uqac.lif.cep.EventTracker;
import ca.uqac.lif.cep.GroupProcessor;
import ca.uqac.lif.cep.functions.ApplyFunction;
import ca.uqac.lif.cep.functions.TurnInto;
import ca.uqac.lif.cep.ltl.Globally;
import ca.uqac.lif.cep.ltl.Next;
import ca.uqac.lif.cep.tmf.Fork;
import ca.uqac.lif.cep.util.Booleans;
import ca.uqac.lif.cep.util.Equals;
import ca.uqac.lif.cep.util.NthElement;
import ca.uqac.lif.cep.util.Numbers;

public class LtlProperty extends GroupProcessor
{
  public static final transient String NAME = "LTL property";
  
  public LtlProperty(EventTracker t)
  {
    super(1, 1);
    Fork f1 = new Fork(2);
    ApplyFunction get_p = new ApplyFunction(new NthElement(2));
    ApplyFunction get_a = new ApplyFunction(new NthElement(1));
    Connector.connect(t, f1, 0, get_p, 0);
    Connector.connect(t, f1, 1, get_a, 0);
    Fork f2 = new Fork(2);
    Connector.connect(t, get_p, 0, f2, 0);
    ApplyFunction lt = new ApplyFunction(Numbers.isLessThan);
    Connector.connect(t, f2, 0, lt, 0);
    TurnInto zero = new TurnInto(0);
    Connector.connect(t, f2, 1, zero, 0);
    Connector.connect(zero, 0, lt, 1);
    
    Fork f3 = new Fork(2);
    Connector.connect(t, get_a, 0, f3, 0);
    ApplyFunction eq = new ApplyFunction(Equals.instance);
    Connector.connect(t, f3, 0, eq, 0);
    TurnInto tia = new TurnInto("a");
    Connector.connect(t, f3, 1, tia, 0);
    Connector.connect(tia, 0, eq, 1);
    Next nx1 = new Next();
    Connector.connect(t, eq, nx1);
    
    Fork f4 = new Fork(2);
    Connector.connect(t, nx1, f4);
    ApplyFunction and1 = new ApplyFunction(Booleans.and);
    Connector.connect(t, f4, 0, and1, 0);
    Next nx2 = new Next();
    Connector.connect(t, f4, 1, nx2, 0);
    Connector.connect(t, nx2, 0, and1, 1);
    
    ApplyFunction and2 = new ApplyFunction(Booleans.and);
    Connector.connect(t, lt, 0, and2, 0);
    Connector.connect(t, and1, 0, and2, 1);
    Globally g = new Globally();
    Connector.connect(t, and2, g);
    
    addProcessors(f1, f2, f3, f4, and1, and2, nx1, nx2, eq, lt, zero, tia, get_p, get_a);
    setEventTracker(t);
    associateInput(0, f1, 0);
    associateOutput(0, g, 0);
  }
  
  @Override
  public LtlProperty duplicate(boolean with_state)
  {
    return new LtlProperty(m_eventTracker.getCopy());
  }
}
