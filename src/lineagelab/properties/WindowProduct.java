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

import ca.uqac.lif.cep.EventTracker;
import ca.uqac.lif.cep.GroupProcessor;
import ca.uqac.lif.cep.functions.Cumulate;
import ca.uqac.lif.cep.functions.CumulativeFunction;
import ca.uqac.lif.cep.tmf.Window;
import ca.uqac.lif.cep.util.Numbers;

import static ca.uqac.lif.cep.Connector.INPUT;
import static ca.uqac.lif.cep.Connector.OUTPUT;

public class WindowProduct extends GroupProcessor
{
  public static final transient String NAME = "Window product";
  
  public WindowProduct(EventTracker t)
  {
    super(1, 1);
    Cumulate prod = new Cumulate(new CumulativeFunction<Number>(Numbers.multiplication));
    Window win = new Window(prod, 3);
    addProcessors(win);
    associateInput(INPUT, win, INPUT);
    associateOutput(OUTPUT, win, OUTPUT);
    setEventTracker(t);
  }
}
