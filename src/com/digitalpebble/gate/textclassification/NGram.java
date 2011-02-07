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
import gate.Factory;
import gate.FeatureMap;
import gate.Node;
import gate.ProcessingResource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ExecutionException;
import gate.util.OffsetComparator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

// creating N-grams can be done easily in JAPE but here we want to handle 
// situations where different annotations are overlapping e.g POS + form + other semantic information
// limit computation of N-Grams to a given span, e.g. sentence

// TODO specify scope :e.g. all bi-grams appearing within a range of 5 tokens

public class NGram extends AbstractLanguageAnalyser implements
		ProcessingResource {
	private String inputAnnotationSet;
	private String outputAnnotationSet;
	private String inputAnnotationType;
	private String inputAnnotationFeature = "label";
	private String outputAnnotationType;
	private String outputAnnotationFeature = "label";
	private Integer ngram;
	private Boolean generateIntermediateAnnotations;
	private Integer window;
	private String spanAnnotationType;
	private String ngramSeparator = "_";

	public void execute() throws ExecutionException {

		// get all the annotations we need from the input AS
		AnnotationSet inputAS = inputAnnotationSet == null
				|| inputAnnotationSet.trim().length() == 0 ? document
				.getAnnotations() : document.getAnnotations(inputAnnotationSet);
		AnnotationSet outputAS = outputAnnotationSet == null
				|| outputAnnotationSet.trim().length() == 0 ? document
				.getAnnotations() : document
				.getAnnotations(outputAnnotationSet);

		// no spans?
		if (getSpanAnnotationType() == null
				| getSpanAnnotationType().equals("")) {
			AnnotationSet inputs = inputAS.get(inputAnnotationType);
			List<Annotation> list = new ArrayList<Annotation>();
			list.addAll(inputs);
			Collections.sort(list, new OffsetComparator());
			// use window or normal
			if (window == -1)
				generateNGrams(list, outputAS);
			else
				generateNGramsOverWindow(list, outputAS);
		} else {
			// use the spans
			AnnotationSet spans = inputAS.get(getSpanAnnotationType());
			Iterator spaniter = spans.iterator();
			while (spaniter.hasNext()) {
				Annotation span = (Annotation) spaniter.next();
				AnnotationSet inputs = inputAS.get(inputAnnotationType, span
						.getStartNode().getOffset(), span.getEndNode()
						.getOffset());
				List<Annotation> list = new ArrayList<Annotation>();
				list.addAll(inputs);
				Collections.sort(list, new OffsetComparator());
				if (window == -1)
					generateNGrams(list, outputAS);
				else
					generateNGramsOverWindow(list, outputAS);
			}
		}
	}

	// we want to take into account overlapping annotations so we create 'boxes'
	// which correspond
	// to a position in the text and has an array of annotations
	private List<List> generateBoxes(List<Annotation> list,
			AnnotationSet outputAS) throws ExecutionException {
		List<List> boxes = new ArrayList<List>();
		Iterator<Annotation> sorted = list.iterator();
		Node previousStart = null;
		Node previousEnd = null;
		List<Annotation> currentList = null;
		while (sorted.hasNext()) {
			Annotation current = sorted.next();
			if (current.getStartNode().equals(previousStart)
					&& current.getEndNode().equals(previousEnd)) {
				// same box
			} else {
				if (currentList != null)
					boxes.add(currentList);
				currentList = new ArrayList<Annotation>();
			}
			previousStart = current.getStartNode();
			previousEnd = current.getEndNode();
			currentList.add(current);
		}
		if (currentList != null)
			boxes.add(currentList);
		return boxes;
	}

	private void generateNGrams(List<Annotation> list, AnnotationSet outputAS)
			throws ExecutionException {
		List<List> boxes = generateBoxes(list, outputAS);

		try {
			// now do the actual n-grams
			for (int b = 0; b < boxes.size(); b++) {
				List<String> tempAnnotationsStartingHere = new ArrayList<String>();
				Long loStart = null;
				Long hiEnd = null;
				for (int z = 0; z < this.ngram.intValue()
						&& (b + z < boxes.size()); z++) {
					// do the combination and dump what we've done at every step
					// e.g generate 1 grams as well as 2-grams
					List<Annotation> current = boxes.get(b + z);
					List<String> temptemp = new ArrayList<String>();
					for (Annotation newAnn : current) {
						// remembering positions
						if (loStart == null)
							loStart = newAnn.getStartNode().getOffset();
						if (hiEnd == null)
							hiEnd = newAnn.getEndNode().getOffset();
						else if (newAnn.getEndNode().getOffset().longValue() > hiEnd
								.longValue())
							hiEnd = newAnn.getEndNode().getOffset();

						String newString = (String) newAnn.getFeatures().get(
								inputAnnotationFeature);
						// TODO : what if there is no such value????
						if (tempAnnotationsStartingHere.size() == 0) {
							// create an annotation for the current annotation
							if (this.generateIntermediateAnnotations) {
								FeatureMap fm = Factory.newFeatureMap();
								fm.put(this.outputAnnotationFeature, newString);
								outputAS.add(loStart, hiEnd,
										outputAnnotationType, fm);
							}
							// add it to the temp
							temptemp.add(newString);
						} else
							for (String existing : tempAnnotationsStartingHere) {
								String combination = existing + getNgramSeparator() + newString;
								temptemp.add(combination);

								if (this.generateIntermediateAnnotations
										| z == this.ngram.intValue() - 1) {
									// create an annotation for the combination
									FeatureMap fm = Factory.newFeatureMap();
									fm.put(this.outputAnnotationFeature,
											combination);
									outputAS.add(loStart, hiEnd,
											outputAnnotationType, fm);
								}
							}
					}
					tempAnnotationsStartingHere = temptemp;
				}
			}
		} catch (Exception e) {
			throw new ExecutionException(e);
		}
	}

	// generate annotations for ngrams over a larger span e.g all couples inside
	// a span of 5 tokens
	// this allows to match more variants e.g. with adjectives in the middle
	// we do not generate intermediate annotations here
	// do with only bigrams for the moment
	private void generateNGramsOverWindow(List<Annotation> list,
			AnnotationSet outputAS) throws ExecutionException {
		List<List> boxes = generateBoxes(list, outputAS);
		try {
			for (int b = 0; b < boxes.size(); b++) {
				List<String> tempAnnotationsStartingHere = new ArrayList<String>();
				Long loStart = null;
				Long hiEnd = null;

				// create a temporary list containing all the annotations
				// at position 0
				List<Annotation> headannots = boxes.get(b);
				for (Annotation newAnn : headannots) {
					// remembering positions
					loStart = newAnn.getStartNode().getOffset();
					if (hiEnd == null)
						hiEnd = newAnn.getEndNode().getOffset();
					else if (newAnn.getEndNode().getOffset().longValue() > hiEnd
							.longValue())
						hiEnd = newAnn.getEndNode().getOffset();

					String string = (String) newAnn.getFeatures().get(
							inputAnnotationFeature);
					tempAnnotationsStartingHere.add(string);
					
					if (this.generateIntermediateAnnotations) {
						FeatureMap fm = Factory.newFeatureMap();
						fm.put(this.outputAnnotationFeature, string);
						outputAS.add(loStart, hiEnd,
								outputAnnotationType, fm);
					}
				}

				for (int z = 1; z < window && (b + z < boxes.size()); z++) {
					// generate all possible bi-grams
					List<Annotation> current = boxes.get(b + z);
					for (Annotation newAnn : current) {
						// remembering positions
						if (hiEnd == null)
							hiEnd = newAnn.getEndNode().getOffset();
						else if (newAnn.getEndNode().getOffset().longValue() > hiEnd
								.longValue())
							hiEnd = newAnn.getEndNode().getOffset();

						String newString = (String) newAnn.getFeatures().get(
								inputAnnotationFeature);

						// take what is in the buffer
						// and make a new annotation out of that
						for (String s : tempAnnotationsStartingHere) {
							String combination = s + getNgramSeparator() + newString;

							// create an annotation for the combination
							FeatureMap fm = Factory.newFeatureMap();
							fm.put(this.outputAnnotationFeature, combination);
							outputAS.add(loStart, hiEnd, outputAnnotationType,
									fm);
						}
					}
				}
			}
		} catch (Exception e) {
			throw new ExecutionException(e);
		}
	}

	public String getInputAnnotationFeature() {
		return inputAnnotationFeature;
	}

	public void setInputAnnotationFeature(String inputAnnotationFeature) {
		this.inputAnnotationFeature = inputAnnotationFeature;
	}

	public String getInputAnnotationSet() {
		return inputAnnotationSet;
	}

	public void setInputAnnotationSet(String inputAnnotationSet) {
		this.inputAnnotationSet = inputAnnotationSet;
	}

	public String getInputAnnotationType() {
		return inputAnnotationType;
	}

	public void setInputAnnotationType(String inputAnnotationType) {
		this.inputAnnotationType = inputAnnotationType;
	}

	public Integer getNgram() {
		return ngram;
	}

	public void setNgram(Integer ngram) {
		this.ngram = ngram;
	}

	public String getOutputAnnotationFeature() {
		return outputAnnotationFeature;
	}

	public void setOutputAnnotationFeature(String outputAnnotationFeature) {
		this.outputAnnotationFeature = outputAnnotationFeature;
	}

	public String getOutputAnnotationSet() {
		return outputAnnotationSet;
	}

	public void setOutputAnnotationSet(String outputAnnotationSet) {
		this.outputAnnotationSet = outputAnnotationSet;
	}

	public String getOutputAnnotationType() {
		return outputAnnotationType;
	}

	public void setOutputAnnotationType(String outputAnnotationType) {
		this.outputAnnotationType = outputAnnotationType;
	}

	public String getSpanAnnotationType() {
		return spanAnnotationType;
	}

	public void setSpanAnnotationType(String spanAnnotationType) {
		this.spanAnnotationType = spanAnnotationType;
	}

	public Boolean getGenerateIntermediateAnnotations() {
		return generateIntermediateAnnotations;
	}

	public void setGenerateIntermediateAnnotations(
			Boolean generateIntermediateAnnotations) {
		this.generateIntermediateAnnotations = generateIntermediateAnnotations;
	}

	public Integer getWindow() {
		return window;
	}

	public void setWindow(Integer window) {
		this.window = window;
	}

	public String getNgramSeparator() {
		return ngramSeparator;
	}

	public void setNgramSeparator(String ngramSeparator) {
		this.ngramSeparator = ngramSeparator;
	}
}
