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
import gate.FeatureMap;
import gate.ProcessingResource;
import gate.Resource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;
import gate.util.OffsetComparator;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.digitalpebble.classification.Document;
import com.digitalpebble.classification.FileTrainingCorpus;
import com.digitalpebble.classification.Learner;
import com.digitalpebble.classification.Parameters;
import com.digitalpebble.classification.Parameters.WeightingMethod;
import com.digitalpebble.classification.RAMTrainingCorpus;

public class TrainingCorpusCreatorPR extends AbstractLanguageAnalyser
		implements ProcessingResource {
	/** * */
	private Learner creator;
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
	private URL directory;
	private FileTrainingCorpus trainingcorpus;
	private String parameters;
	private Integer minDocThreshold;
	private Integer topNAttributes;
	private String weightingScheme;
	private Boolean reinitModel;
	private String implementation = Learner.LibSVMModelCreator;

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
		if (directory == null)
			throw new ResourceInstantiationException(
					"directory is required to store the data and cannot be null");
		// it is not null, check it is a file: URL
		if (!"file".equals(directory.getProtocol())) {
			throw new ResourceInstantiationException(
					"directory must be a file: URL");
		}
		// initializes the modelCreator
		String pathDirectory = new File(URI.create(directory.toExternalForm()))
				.getAbsolutePath();
		try {
			this.creator = Learner.getLearner(pathDirectory, implementation,
					reinitModel);
			this.trainingcorpus = creator.getFileTrainingCorpus();
		} catch (Exception e) {
			throw new ResourceInstantiationException(e);
		}

		fireProcessFinished();

		System.out.println("ModelCreator reinitialised");

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

		// reinitialise the model if necessary
		int positionDoc = corpus.indexOf(document);
		if (positionDoc == 0 && getReinitModel().booleanValue())
			try {
				reInit();
			} catch (ResourceInstantiationException e1) {
				throw new ExecutionException(e1);
			}

		// check parameters
		checkParameters();
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
			// find out the feature of type textAnnotationValue
			// e.g a sentence
			FeatureMap features = annotation.getFeatures();
			String textAV = (String) features.get(textAnnotationValue);
			if (textAV == null) {
				continue;
			}
			// obtain the components annotations
			AnnotationSet set = inputAS.getContained(annotation.getStartNode()
					.getOffset(), annotation.getEndNode().getOffset());
			if (set.isEmpty())
				continue;
			AnnotationSet underlyingAS = set.get(componentAnnotationType);
			if (underlyingAS.isEmpty())
				continue;

			List<Annotation> list = new ArrayList<Annotation>();
			Iterator<Annotation> iter = underlyingAS.iterator();
			while (iter.hasNext()) {
				Annotation annot = iter.next();
				if (annot.getFeatures().containsKey(componentAnnotationValue)) {
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
						componentAnnotationValue);
			}
			if (values.length == 0)
				continue;
			Document newDocument = creator.createDocument(values, textAV);
			try {
				this.trainingcorpus.addDocument(newDocument);
			} catch (IOException e) {
				throw new ExecutionException(e);
			}
		}
		// check if this document is the last of this corpus in which case
		// we'll start the learning
		fireProgressChanged((100 * positionDoc) / corpus.size());
		// do we trigger the learning?
		if (positionDoc == corpus.size() - 1) {
			fireStatusChanged("Training the Engine");
			fireProgressChanged(0);
			try {
				// initialisation learner
				this.creator.setParameters(getParameters());
				WeightingMethod method = Parameters.WeightingMethod
						.methodFromString(getWeightingScheme());
				this.creator.setMethod(method);
				this.creator.pruneTermsDocFreq(getMinDocThreshold().intValue(),
						Integer.MAX_VALUE);
				if (this.topNAttributes != null)
					this.creator.keepTopNAttributesLLR(this.topNAttributes
							.intValue());
				// this.creator.learn(trainingcorpus);
				trainingcorpus.close();
				creator.saveLexicon();
			} catch (Exception e) {
				throw new ExecutionException(e);
			} finally {
				fireProcessFinished();
			}
		}
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
		// check weighting scheme
		Parameters.WeightingMethod.methodFromString(getWeightingScheme());
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

	public URL getDirectory() {
		return directory;
	}

	public void setDirectory(URL directoryLocation) {
		this.directory = directoryLocation;
	}

	public Integer getMinDocThreshold() {
		return minDocThreshold;
	}

	public Integer getTopNAttributes() {
		return topNAttributes;
	}

	public void setTopNAttributes(Integer topNAttributes) {
		this.topNAttributes = topNAttributes;
	}

	public void setMinDocThreshold(Integer minDocThreshold) {
		this.minDocThreshold = minDocThreshold;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String params) {
		parameters = params;
	}

	public String getWeightingScheme() {
		return weightingScheme;
	}

	public void setWeightingScheme(String weightingScheme) {
		this.weightingScheme = weightingScheme;
	}

	public Boolean getReinitModel() {
		return reinitModel;
	}

	public void setReinitModel(Boolean reinitModel) {
		this.reinitModel = reinitModel;
	}

	public String getImplementation() {
		return implementation;
	}

	public void setImplementation(String implementation) {
		this.implementation = implementation;
	}

}