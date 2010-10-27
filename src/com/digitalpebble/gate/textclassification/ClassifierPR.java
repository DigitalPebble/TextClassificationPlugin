/**
 * Copyright 2010 DigitalPebble Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.digitalpebble.gate.textclassification;

import gate.Annotation;
import gate.AnnotationSet;
import gate.ProcessingResource;
import gate.Resource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.OffsetComparator;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.digitalpebble.classification.Document;
import com.digitalpebble.classification.TextClassifier;

public class ClassifierPR extends AbstractLanguageAnalyser implements
		ProcessingResource {
	/** * */
	private TextClassifier applier;

	/**
	 * Input AnnotationSet name
	 */
	private String inputAnnotationSet;

	/**
	 * Text Annotation Type (e.g. Sentence) For which a separate text
	 * classification document is created
	 */
	private String textAnnotationType;

	/**
	 * Text Annotation Value (e.g. lang) The value of this feature is used as a
	 * label
	 */
	private String textAnnotationValue;

	private String textAnnotationScore = "_score";

	/**
	 * Component Annotation Value (e.g. token) The annotation is considered to
	 * obtain feature values
	 */
	private String componentAnnotationType;

	/**
	 * ComponentAnnotationValue (e.g. form) The value of this feature is
	 * considered as a value for the features
	 */
	private String componentAnnotationValue;

	private URL modelDir;

	/*
	 * this method gets called whenever an object of this class is created
	 * either from GATE GUI or if initiated using Factory.createResource()
	 * method.
	 */
	public Resource init() throws ResourceInstantiationException {
		// here initialize all required variables, and may
		// be throw an exception if the value for any of the
		// mandatory parameters is not provided
		// check that a modelLocation has been selected
		if (modelDir == null)
			throw new ResourceInstantiationException(
					"resourceDir is required to store the learned model and cannot be null");
		// it is not null, check it is a file: URL
		if (!"file".equals(modelDir.getProtocol())) {
			throw new ResourceInstantiationException(
					"resourceDir must be a file: URL");
		}
		// initializes the modelCreator
		try {
			String pathresourceDir = new File(URI.create(modelDir
					.toExternalForm())).getAbsolutePath();
			this.applier = TextClassifier.getClassifier(pathresourceDir);
		} catch (Exception e) {
			throw new ResourceInstantiationException(e);
		}
		fireProcessFinished();
		return this;
	}

	/* this method is called to reinitialize the resource */
	public void reInit() throws ResourceInstantiationException {
		init();
	}

	/**
	 * Called when user clicks on RUN button in GATE GUI
	 */
	public void execute() throws ExecutionException {
		// check parameters
		checkParameters();

		this.fireStatusChanged("TextClassification applied on "
				+ document.getName());

		AnnotationSet inputAS = inputAnnotationSet == null
				|| inputAnnotationSet.trim().length() == 0 ? document
				.getAnnotations() : document.getAnnotations(inputAnnotationSet);
		// obtain annotations of type textAnnotationType
		AnnotationSet textAS = inputAS.get(textAnnotationType);
		if (textAS == null || textAS.isEmpty()) {
			System.err.println("There are no annotations of type "
					+ textAnnotationType + " available in document!");
		}
		Iterator iterator = textAS.iterator();
		while (iterator.hasNext()) {
			Annotation annotation = (Annotation) iterator.next();
			// obtain the underlying annotations
			AnnotationSet underlyingAS = obtainUnderlyingAnnotations(inputAS,
					annotation, componentAnnotationType);
			if (underlyingAS == null || underlyingAS.isEmpty()) {
				continue;
			}
			List list = new ArrayList();
			Iterator iter = underlyingAS.iterator();
			while (iter.hasNext()) {
				Annotation annot = (Annotation) iter.next();
				if (annot.getFeatures().containsKey(componentAnnotationValue)) {
					list.add(annot);
				}
			}
			// make underlyingAS eligible for garbage collection
			underlyingAS = null;
			// sort them
			Collections.sort(list, new OffsetComparator());
			String[] values = new String[list.size()];
			String[] labels = this.applier.getLabels();
			// else obtain the value of each feature (componentAnnotationValue)
			for (int i = 0; i < list.size(); i++) {
				Annotation annot = (Annotation) list.get(i);
				values[i] = (String) annot.getFeatures().get(
						componentAnnotationValue);
			}
			Document newDocument = this.applier.createDocument(values);
			double[] scores = new double[labels.length];
			int bestlabel = 0;
			double bestscore = 0d;
			try {
				scores = this.applier.classify(newDocument);
				for (int l = 0; l < scores.length; l++) {
					if (scores[l] > bestscore) {
						bestscore = scores[l];
						bestlabel = l;
					}
				}
			} catch (Exception e) {
				throw new ExecutionException(e);
			}
			// create a new label for this entity
			// find out the feature of type textAnnotationValue
			annotation.getFeatures()
					.put(textAnnotationValue, labels[bestlabel]);
			annotation.getFeatures().put(
					textAnnotationValue + textAnnotationScore,
					new Double(bestscore));
		}
		fireProcessFinished();
	}

	/**
	 * The method, given input AnnotationSet, parent annotation and the type of
	 * underlying annotations, returns the annotationSet that consists of
	 * underlying annotations withing the parent's annotation's boundaries
	 * 
	 * @param inputAS
	 * @param parent
	 * @param annotationType
	 * @return
	 */
	private AnnotationSet obtainUnderlyingAnnotations(AnnotationSet inputAS,
			Annotation parent, String annotationType) {
		AnnotationSet set = inputAS.getContained(parent.getStartNode()
				.getOffset(), parent.getEndNode().getOffset());
		if (set == null || set.isEmpty()) {
			return null;
		}
		return set.get(annotationType);
	}

	/**
	 * Checks if values for the manadatory parameters provided.
	 * 
	 * @throws ExecutionException
	 */
	private void checkParameters() throws ExecutionException {
		if (document == null)
			throw new ExecutionException("Document is null!");
		if (textAnnotationType == null
				|| textAnnotationType.trim().length() == 0)
			throw new ExecutionException("TextAnnotationType is null!");
		if (textAnnotationValue == null
				|| textAnnotationValue.trim().length() == 0)
			throw new ExecutionException("TextAnnotationValue is null!");
		if (componentAnnotationType == null
				|| componentAnnotationType.trim().length() == 0)
			throw new ExecutionException("componentAnnotationType is null!");
		if (componentAnnotationValue == null
				|| componentAnnotationValue.trim().length() == 0)
			throw new ExecutionException("componentAnnotationValue is null!");
	}

	public String getComponentAnnotationType() {
		return componentAnnotationType;
	}

	public void setComponentAnnotationType(String componentAnnotationType) {
		this.componentAnnotationType = componentAnnotationType;
	}

	public String getComponentAnnotationValue() {
		return componentAnnotationValue;
	}

	public void setComponentAnnotationValue(String componentAnnotationValue) {
		this.componentAnnotationValue = componentAnnotationValue;
	}

	public String getInputAnnotationSet() {
		return inputAnnotationSet;
	}

	public void setInputAnnotationSet(String inputAnnotationSet) {
		this.inputAnnotationSet = inputAnnotationSet;
	}

	public String getTextAnnotationType() {
		return textAnnotationType;
	}

	public void setTextAnnotationType(String textAnnotationType) {
		this.textAnnotationType = textAnnotationType;
	}

	public String getTextAnnotationValue() {
		return textAnnotationValue;
	}

	public void setTextAnnotationValue(String textAnnotationValue) {
		this.textAnnotationValue = textAnnotationValue;
	}

	public URL getModelDir() {
		return modelDir;
	}

	public void setResourceDir(URL modelDir) {
		this.modelDir = modelDir;
	}
}
