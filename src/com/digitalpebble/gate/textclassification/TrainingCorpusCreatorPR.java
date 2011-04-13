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
import java.util.Map;

import com.digitalpebble.classification.Document;
import com.digitalpebble.classification.FileTrainingCorpus;
import com.digitalpebble.classification.Learner;
import com.digitalpebble.classification.Lexicon;
import com.digitalpebble.classification.Parameters;
import com.digitalpebble.classification.TrainingCorpus;
import com.digitalpebble.classification.Parameters.WeightingMethod;
import com.digitalpebble.classification.RAMTrainingCorpus;
import com.digitalpebble.classification.util.CorpusUtils;
import com.digitalpebble.classification.util.scorers.AttributeScorer;
import com.digitalpebble.classification.util.scorers.logLikelihoodAttributeScorer;
import com.digitalpebble.classification.libsvm.Utils;

public class TrainingCorpusCreatorPR extends AbstractLanguageAnalyser
		implements ProcessingResource {
	/** * */
	private Learner creator;
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
	 * Attribute Annotation Type (e.g. token) The annotation type used for the ML attributes.
	 */
	private String attributeAnnotationType;
	/**
	 * ComponentAnnotationValue (e.g. form) The feature value used for the ML attributes.
	 */
	private String attributeAnnotationValue;
	/**
	 * Directory where lexicon, vector and raw model will be saved
	 */
	private URL directory;
	/**
	 * FEature weighting scheme
	 */
	private String weightingScheme;

	private int minFreq=1;
	private int maxFreq=Integer.MAX_VALUE;
	/**
	 * Run after prunning according to min and max freq
	 */
	private int keepNBestAttributes =0;
	/**
	 * Compact the lexicon after prunning
	 */
	boolean compactLexicon =true;
	
	private Boolean reinitCorpus = true;
	
	private FileTrainingCorpus trainingcorpus;
	private String implementation = Learner.LibSVMModelCreator;
	String pathDirectory;
	private String libsvmVectorPath;

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
		if (directory == null || "".equals(directory))
			throw new ResourceInstantiationException(
					"directory is required to store the data and cannot be null or empty");
		// it is not null, check it is a file: URL
		if (!"file".equals(directory.getProtocol())) {
			throw new ResourceInstantiationException(
					"directory must be a file: URL");
		}
		
		// initializes the modelCreator
		pathDirectory = new File(URI.create(directory.toExternalForm()))
				.getAbsolutePath();
		if(libsvmVectorPath == null || libsvmVectorPath.isEmpty()){
			libsvmVectorPath = pathDirectory+File.separator+"vector"; 
		}
		
		
		try {
			this.creator = Learner.getLearner(pathDirectory, implementation,
					reinitCorpus);
			this.trainingcorpus = creator.getFileTrainingCorpus();
		} catch (Exception e) {
			throw new ResourceInstantiationException(e);
		}

		fireProcessFinished();

		System.out.println("CorpusCreator reinitialised");

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
		if (positionDoc == 0 && getReinitCorpus().booleanValue())
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
		AnnotationSet textAS = inputAS.get(labelAnnotationType);
		if (textAS == null || textAS.isEmpty()) {
			System.err.println("There are no annotations of type "
					+ labelAnnotationType + " available in document!");
		}
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
			}
			if (values.length == 0)
				continue;
			// creates a simple document
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
			fireStatusChanged("Saving the Lexicon");
			try {
				WeightingMethod method = Parameters.WeightingMethod
						.methodFromString(getWeightingScheme());
				this.creator.setMethod(method);
				trainingcorpus.close();
				Lexicon lexicon = creator.getLexicon();
				creator.saveLexicon();
				//prune by frequency
				lexicon.pruneTermsDocFreq(minFreq, maxFreq);
				//further keep only the N best attributes
				if (keepNBestAttributes >0) {	
					AttributeScorer scorer = logLikelihoodAttributeScorer.getScorer(
							trainingcorpus, lexicon);
					lexicon.setAttributeScorer(scorer);
					lexicon.applyAttributeFilter(scorer, keepNBestAttributes);
				} 
				// change the indices of the attributes to remove 
				// gaps between them
				Map<Integer, Integer> equiv = null;
				if (compactLexicon){
					// create a new Lexicon object
					equiv = lexicon.compact();
				}
				// save the modified lexicon file
				lexicon.saveToFile(this.pathDirectory+"lexicon.compact");
				Utils.writeExamples(trainingcorpus,lexicon,
						this.libsvmVectorPath, equiv);
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
		if (labelAnnotationType == null
				|| labelAnnotationType.trim().length() == 0)
			throw new ExecutionException("TextAnnotationType is null!");
		if (labelAnnotationValue == null
				|| labelAnnotationValue.trim().length() == 0)
			throw new ExecutionException("TextAnnotationValue is null!");
		if (attributeAnnotationType == null
				|| attributeAnnotationType.trim().length() == 0)
			throw new ExecutionException("componentAnnotationType is null!");
		if (attributeAnnotationValue == null
				|| attributeAnnotationValue.trim().length() == 0)
			throw new ExecutionException("componentAnnotationValue is null!");
		// check weighting scheme
		Parameters.WeightingMethod.methodFromString(getWeightingScheme());
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

	public URL getDirectory() {
		return directory;
	}

	public void setDirectory(URL directoryLocation) {
		this.directory = directoryLocation;
	}

	public String getWeightingScheme() {
		return weightingScheme;
	}

	public void setWeightingScheme(String weightingScheme) {
		this.weightingScheme = weightingScheme;
	}

	public Boolean getReinitCorpus() {
		return reinitCorpus;
	}

	public void setReinitCorpus(Boolean reinitCorpus) {
		this.reinitCorpus = reinitCorpus;
	}

	public String getImplementation() {
		return implementation;
	}

	public void setImplementation(String implementation) {
		this.implementation = implementation;
	}

}