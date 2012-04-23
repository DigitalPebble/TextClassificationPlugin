package com.digitalpebble.gate.mahout;

import gate.Annotation;
import gate.AnnotationSet;
import gate.FeatureMap;
import gate.ProcessingResource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.util.OffsetComparator;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

// Generates a sequencial representation of the documents 
// and relies on Mahout for vectorizing them.
// The output is simply a SequenceFile<Text>,<Text>

public class SequenceFileGenerator extends AbstractLanguageAnalyser implements
		ProcessingResource {

	/**
	 * Input AnnotationSet name
	 */
	private String inputAnnotationSet;
	/**
	 * Label Annotation Type (e.g. Sentence) For which a separate text
	 * classification document is created
	 */
	private String labelAnnotationType;
	/**
	 * Label Annotation Value (e.g. lang) The value of this feature is used as a
	 * label
	 */
	private String labelAnnotationValue;
	/**
	 * Attribute Annotation Type (e.g. token) The annotation type used for the
	 * ML attributes.
	 */
	private String attributeAnnotationType;
	/**
	 * ComponentAnnotationValue (e.g. form) The feature value used for the ML
	 * attributes.
	 */
	private String attributeAnnotationValue;
	private String outpath;

	private SequenceFile.Writer writer;

	/**
	 * Called when user clicks on RUN button in GATE GUI
	 */
	public void execute() throws ExecutionException {

		// reinitialise the model if necessary
		int positionDoc = corpus.indexOf(document);
		if (positionDoc == 0)
			try {
				Path path = new Path(this.outpath);
				Configuration conf = new Configuration(true);
				InputStream is = this.getClass().getClassLoader().getResourceAsStream("core-default.xml");
				conf.addResource(is);
				FileSystem fs = FileSystem.get(conf);
				writer = new SequenceFile.Writer(fs, conf, path, Text.class,
						Text.class);
			} catch (IOException e) {
				throw new ExecutionException(e);
			}

		AnnotationSet inputAS = inputAnnotationSet == null
				|| inputAnnotationSet.trim().length() == 0 ? document
				.getAnnotations() : document.getAnnotations(inputAnnotationSet);
		// obtain annotations of type textAnnotationType
		AnnotationSet textAS = inputAS.get(labelAnnotationType);
		if (textAS == null || textAS.isEmpty()) {
			System.err.println("There are no annotations of type "
					+ labelAnnotationType + " available in document!");
		}
		int subDoc = 0;
		Iterator<Annotation> iterator = textAS.iterator();
		while (iterator.hasNext()) {
			Annotation annotation = iterator.next();
			// find out the feature of type textAnnotationValue
			// e.g a sentence
			FeatureMap features = annotation.getFeatures();
			String textAV = (String) features.get(labelAnnotationValue);
			if (textAV == null) {
				continue;
			}
			// obtain the components annotations
			AnnotationSet set = inputAS.getContained(annotation.getStartNode()
					.getOffset(), annotation.getEndNode().getOffset());
			if (set.isEmpty())
				continue;
			AnnotationSet underlyingAS = set.get(attributeAnnotationType);
			if (underlyingAS.isEmpty())
				continue;

			StringBuffer textRep = new StringBuffer();

			List<Annotation> list = new ArrayList<Annotation>();
			Iterator<Annotation> iter = underlyingAS.iterator();
			while (iter.hasNext()) {
				Annotation annot = iter.next();
				if (annot.getFeatures().containsKey(attributeAnnotationValue)) {
					list.add(annot);
				}
			}
			// make underlyingAS eligible for garbage collection
			underlyingAS = null;
			// sort them
			Collections.sort(list, new OffsetComparator());
			String[] values = new String[list.size()];
			// else obtain the value of each feature (componentAnnotationValue)
			for (int i = 0; i < list.size(); i++) {
				Annotation annot = (Annotation) list.get(i);
				values[i] = (String) annot.getFeatures().get(
						attributeAnnotationValue);
				textRep.append(values[i]).append(" ");
			}
			if (values.length == 0)
				continue;
			// creates a simple text representation of the document
			++subDoc;
			try {
				writer.append(new Text(document.getName() + "_" + subDoc),
						new Text(textRep.toString()));
			} catch (IOException e) {
				throw new ExecutionException(e);
			}
		}
		// check if this document is the last of this corpus in which case
		// we'll start the learning
		fireProgressChanged((100 * positionDoc) / corpus.size());
		// do we trigger the learning?
		if (positionDoc == corpus.size() - 1) {
			fireStatusChanged("Closing the writer");
			try {
				// close writer
				writer.close();
			} catch (Exception e) {
				throw new ExecutionException(e);
			} finally {
				fireProcessFinished();
			}
		}
	}

	public String getAttributeAnnotationType() {
		return attributeAnnotationType;
	}

	public void setAttributeAnnotationType(String componentAnnotationType) {
		this.attributeAnnotationType = componentAnnotationType;
	}

	public String getAttributeAnnotationValue() {
		return attributeAnnotationValue;
	}

	public void setAttributeAnnotationValue(String componentAnnotationValue) {
		this.attributeAnnotationValue = componentAnnotationValue;
	}

	public String getInputAnnotationSet() {
		return inputAnnotationSet;
	}

	public void setInputAnnotationSet(String inputAnnotationSet) {
		this.inputAnnotationSet = inputAnnotationSet;
	}

	public String getLabelAnnotationType() {
		return labelAnnotationType;
	}

	public void setLabelAnnotationType(String textAnnotationType) {
		this.labelAnnotationType = textAnnotationType;
	}

	public String getLabelAnnotationValue() {
		return labelAnnotationValue;
	}

	public void setLabelAnnotationValue(String textAnnotationValue) {
		this.labelAnnotationValue = textAnnotationValue;
	}

	public String getOutpath() {
		return outpath;
	}

	public void setOutpath(String path) {
		this.outpath = path;
	}

}
