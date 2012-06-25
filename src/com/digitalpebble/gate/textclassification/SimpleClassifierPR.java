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

import gate.ProcessingResource;
import gate.Resource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.creole.ResourceInstantiationException;

import java.io.File;
import java.net.URI;
import java.net.URL;

import com.digitalpebble.classification.Document;
import com.digitalpebble.classification.TextClassifier;
import com.digitalpebble.classification.util.Tokenizer;

/**
 * Uses the entire text of a document and stores the value in a document feature
 **/

public class SimpleClassifierPR extends AbstractLanguageAnalyser implements
		ProcessingResource {
	/** * */
	private TextClassifier applier;

	private String featureNameForLabel;

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

		String text = getDocument().getContent().toString();

		// create a document from a String
		String[] tokens = Tokenizer.tokenize(text.toString(), true);
		Document doc = this.applier.createDocument(tokens);
		// classify
		try {
			double[] scores = this.applier.classify(doc);
			// get best label
			String label = this.applier.getBestLabel(scores);
			getDocument().getFeatures().put(featureNameForLabel, label);
		} catch (Exception e) {
			e.printStackTrace();
		}

		fireProcessFinished();
	}

	/**
	 * Checks if values for the manadatory parameters provided.
	 * 
	 * @throws ExecutionException
	 */
	private void checkParameters() throws ExecutionException {
		if (document == null)
			throw new ExecutionException("Document is null!");
		if (featureNameForLabel == null
				|| featureNameForLabel.trim().length() == 0)
			throw new ExecutionException("TextAnnotationValue is null!");
	}

	public String getFeatureNameForLabel() {
		return featureNameForLabel;
	}

	public void setFeatureNameForLabel(String textAnnotationValue) {
		this.featureNameForLabel = textAnnotationValue;
	}

	public URL getModelDir() {
		return modelDir;
	}

	public void setModelDir(URL modelDir) {
		this.modelDir = modelDir;
	}
}
