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
package lineagelab;

import ca.uqac.lif.cep.EventTracker;
import ca.uqac.lif.cep.Processor;
import ca.uqac.lif.cep.provenance.IndexEventTracker;
import ca.uqac.lif.json.JsonTrue;
import ca.uqac.lif.labpal.ExperimentFactory;
import ca.uqac.lif.labpal.Region;
import lineagelab.properties.CvcProcedure;
import lineagelab.properties.LtlProperty;
import lineagelab.properties.Payment;
import lineagelab.properties.ProcessLifecycle;
import lineagelab.properties.WindowProduct;
import lineagelab.source.CvcSource;
import lineagelab.source.ProcessSource;
import lineagelab.source.RandomNumberSource;
import lineagelab.source.XesSource;

import static lineagelab.StreamExperiment.LINEAGE;
import static lineagelab.StreamExperiment.PROPERTY;

/**
 * An {@link ExperimentFactory} that produces {@link StreamExperiment}s.
 */
@SuppressWarnings("rawtypes")
public class StreamExperimentFactory extends ExperimentFactory<LineageLab,StreamExperiment>
{
  public StreamExperimentFactory(LineageLab lab)
  {
    super(lab, StreamExperiment.class);
  }

  @Override
  protected StreamExperiment<?> createExperiment(Region r)
  {
    boolean lineage = (r.get(LINEAGE) instanceof JsonTrue);
    StreamExperiment<?> exp = new StreamExperiment(lineage);
    setSource(r, exp);
    exp.setEventStep(LineageLab.s_eventStep);
    Processor p = setProcessor(exp, r, lineage);
    if (p == null)
    {
      return null;
    }
    exp.setInput(PROPERTY, r.getString(PROPERTY));
    exp.setPropertyDescription(getPropertyDescription(r));
    exp.setImageUrl(getImageUrl(r));
    exp.setPredictedThroughput(guessThroughput(r));
    return exp;
  }
  
  @SuppressWarnings("unchecked")
  protected void setSource(Region r, StreamExperiment exp)
  {
    String property_name = r.getString(PROPERTY);
    if (property_name == null)
    {
      return;
    }
    if (property_name.compareTo(WindowProduct.NAME) == 0)
    {
      exp.setSource(new RandomNumberSource(m_lab.getRandom(), LineageLab.MAX_TRACE_LENGTH));
    }
    if (property_name.compareTo(ProcessLifecycle.NAME) == 0)
    {
      exp.setSource(new ProcessSource(m_lab.getRandom(), LineageLab.MAX_TRACE_LENGTH, 100));
    }
    if (property_name.compareTo(LtlProperty.NAME) == 0)
    {
      exp.setSource(new ProcessSource(m_lab.getRandom(), LineageLab.MAX_TRACE_LENGTH, 100));
    }
    if (property_name.compareTo(CvcProcedure.NAME) == 0)
    {
      exp.setSource(new CvcSource(LineageLab.MAX_TRACE_LENGTH));
    }
    if (property_name.compareTo(Payment.NAME) == 0)
    {
      exp.setSource(new XesSource(LineageLab.MAX_TRACE_LENGTH));
    }
  }

  protected Processor setProcessor(StreamExperiment exp, Region r, boolean lineage)
  {
    String property_name = r.getString(PROPERTY);
    Processor p = null;
    EventTracker t = null;
    if (lineage)
    {
      t = new IndexEventTracker();
    }
    exp.setTracker(t);
    if (property_name == null)
    {
      return null;
    }
    if (property_name.compareTo(WindowProduct.NAME) == 0)
    {
      p = new WindowProduct(t);
    }
    if (property_name.compareTo(ProcessLifecycle.NAME) == 0)
    {
      p = new ProcessLifecycle(t);
    }
    if (property_name.compareTo(LtlProperty.NAME) == 0)
    {
      p = new LtlProperty(t);
    }
    if (property_name.compareTo(CvcProcedure.NAME) == 0)
    {
      p = new CvcProcedure(t);
    }
    if (property_name.compareTo(Payment.NAME) == 0)
    {
      p = new Payment(t);
    }
    exp.setProcessor(p);
    return p;
  }

  protected String getPropertyDescription(Region r)
  {
    String property_name = r.getString(PROPERTY);
    if (property_name == null)
    {
      return null;
    }
    if (property_name.compareTo(WindowProduct.NAME) == 0)
    {
      return "It computes a product over a sliding window of numbers.";
    }
    if (property_name.compareTo(ProcessLifecycle.NAME) == 0)
    {
      return "It checks that each process instance follows a lifecycle defined by a Moore machine.";
    }
    if (property_name.compareTo(CvcProcedure.NAME) == 0)
    {
      return "It checks that a log of actions follows the procedure for installing a Central Veinous Catheter.";
    }
    if (property_name.compareTo(Payment.NAME) == 0)
    {
      return "It checks the maximum duration of a payment process instance.";
    }
    return null;
  }

  protected String getImageUrl(Region r)
  {
    String property_name = r.getString(PROPERTY);
    if (property_name == null)
    {
      return null;
    }
    /*if (property_name.compareTo(ParcelsInTransit.NAME) == 0)
    {
      // No picture for this one
      return "/resource/ParcelsInTransit.png";
    }*/
    return null;
  }

  /**
   * Estimates the throughput of an experiment.
   * These throughput values are rough estimates based on values
   * collected when running the experiments.
   * @param r The region representing an experiment
   * @return The estimated throughput
   */
  protected float guessThroughput(Region r)
  {
    String property_name = r.getString(PROPERTY);
    if (property_name == null)
    {
      return 0f;
    }
    /*if (property_name.compareTo(DecreasingDistance.NAME) == 0)
    {
      return 300000f;
    }*/
    return 0f;
  }
}
