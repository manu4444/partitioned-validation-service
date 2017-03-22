package util;

import java.io.*;
import java.util.*;

//import com.sun.org.apache.xalan.internal.xsltc.runtime.Hashtable;

public class StatContainer implements Serializable{
	public static final int ASC = 0;
	public static final int DESC = 1;	
	private float sum;
	private float squaredSum;
	float numSamples;
	float mean;
	float median;
	float stdDev;
	float min;
	float max;
	Vector<Float> values;
	Vector<Float> sortedValues;
	boolean sorted = false;
	boolean medianCalculated = false;
	int samplesLimit = -1;
	public StatContainer(){
		sum = 0;
		squaredSum = 0;
		numSamples = 0;
		mean = 0;
		stdDev = 0;
		min = 0;
		max = 0;
		sorted = false;
		medianCalculated = false;
		values = new Vector<Float>();
		sortedValues = new Vector<Float>();
	}
	
	public void add(float value) {
		values.add(value);
		sortedValues.add(value);
		sorted=false;
		// calculate min and max
		numSamples++;
		if (numSamples == 1) {
			min = value;
			max = value;
		}
		else if (value > max) max = value;
		else if (value < min) min = value;
		
		// calculate mean
		sum += value;
		squaredSum += value * value;
		mean = sum / numSamples;
		// calculate standard deviation
		if(numSamples > 1) {
			float variance = (squaredSum - numSamples * mean * mean)/(numSamples -1);
			stdDev = (float) Math.sqrt((double)variance);
		}
		if(samplesLimit != -1){
            if(values.size()>samplesLimit){
                    remove();
            }
        }

	}
	
	public void remove() {
		Float deletedValue = values.firstElement();
		numSamples--;
		sum = sum - deletedValue.floatValue();
		squaredSum = squaredSum - (deletedValue.floatValue() * deletedValue.floatValue());
		mean = sum / numSamples;
		if (numSamples > 1) {
            float variance = (squaredSum - numSamples * mean * mean)/(numSamples -1);
            stdDev = (float) Math.sqrt((double)variance);
        }
		values.remove(deletedValue);
		sortedValues.remove(deletedValue);
	}


	public float getNumSamples() {
		return numSamples;
	}

	public float getMean() {
		return mean;
	}

	public void sortValues(){
		if(!sorted) {
			Collections.sort(sortedValues);
			sorted = true;
		}
	}
	
	public float getMedian() {
		sortValues();	
		int size = sortedValues.size();	
		if(size==0)
			return 0;
		else if(size==1)
			return sortedValues.get(0);

		else if (size % 2 == 1) {
			median = ((Float)sortedValues.get((size + 1) / 2 - 1)).floatValue();
		}
		else {
			median = (((Float)sortedValues.get(size / 2 - 1)).floatValue() + ((Float)sortedValues.get(size / 2)).floatValue()) / 2;
		}
		return median;
	}
	
	public float getMin() {
		return min;
	}
	
	public float getMax() {
		return max;
	}
	
	public float getStdDev() {
		return stdDev;
	}

	public float getXthPercentile(int x, int mode) {
		if(values.size()==0)
                return 0;
        else if(values.size()==1)
                return values.get(0);

		sortValues();	
		if(mode == DESC)
			x = 100- x;
		
		int idx = (int)(((float)x/100) * sortedValues.size());	
		return sortedValues.get(idx).floatValue();
	}
	
	public Vector<Float> getValues(){
		return values;
	} 	

	public Vector<Float> getSortedValues(){
		sortValues();
		return sortedValues;
	}

	public void reset(){
		sum = 0;
		squaredSum = 0;
		numSamples = 0;
		mean = 0;
		stdDev = 0;
	}
	
	public float getSum(){
		return sum;
	}

	public void setSamplesLimit(int samplesLimit) {
        this.samplesLimit = samplesLimit;
    }

}
