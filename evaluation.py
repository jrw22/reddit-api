#### SENTIMENT ANALYSIS - PRECISION, RECALL AND F1 ####

from sklearn.metrics import precision_score, recall_score, f1_score, classification_report
true_labels = ['positive', 'negative', 'neutral', 'positive', 'negative']  # Example true labels
predicted_labels = ['positive', 'neutral', 'neutral', 'positive', 'negative']  # Example predicted labels

# Calculate precision, recall, and f1 score for each class
precision = precision_score(true_labels, predicted_labels, average=None, labels=['negative', 'neutral', 'positive'])
recall = recall_score(true_labels, predicted_labels, average=None, labels=['negative', 'neutral', 'positive'])
f1 = f1_score(true_labels, predicted_labels, average=None, labels=['negative', 'neutral', 'positive'])

# Print scores for each class
print("Precision:", precision)
print("Recall:", recall)
print("F1 Score:", f1)

# Generate a classification report
report = classification_report(true_labels, predicted_labels, target_names=['negative', 'neutral', 'positive'])
print(report)


#### SUMMARISATION - COMPARING SUMMARIES TO HUMAN GENERATED ONES ####

from rouge import Rouge
from bert_score import score as bert_score
import sacrebleu
import textstat
import nlg_eval

def evaluate_summary(reference, generated):
    results = {}

    # ROUGE Scores
    rouge = Rouge()
    rouge_scores = rouge.get_scores(generated, reference, avg=True)
    results['ROUGE'] = rouge_scores

    # BLEU Score
    bleu = sacrebleu.corpus_bleu([generated], [[reference]])
    results['BLEU'] = bleu.score

    # BERTScore
    P, R, F1 = bert_score([generated], [reference], lang="en", verbose=True)
    results['BERTScore'] = {'Precision': P.mean().item(), 'Recall': R.mean().item(), 'F1': F1.mean().item()}

    # Readability
    readability_score = textstat.flesch_reading_ease(generated)
    results['Readability'] = readability_score

    return results

# Example usage
reference_summary = "This is the human reference summary."
generated_summary = "This is the generated summary."

evaluation_results = evaluate_summary(reference_summary, generated_summary)
print(evaluation_results)


#### TOPIC MODELLING - COHERENCE SCORES ####

from gensim.models import CoherenceModel

# Assuming `model` is your trained topic model and `texts` is your preprocessed text data
topics = model.show_topics(formatted=False)
coherence_model = CoherenceModel(topics=topics, texts=texts, dictionary=dictionary, coherence='c_v')
coherence_score = coherence_model.get_coherence()

print("Coherence Score:", coherence_score)
