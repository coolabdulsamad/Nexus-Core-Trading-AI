"""
src/news_engine/sentiment.py
Responsibility: Load FinBERT and analyze sentiment of headlines.
FIXED: Uses explicit BertForSequenceClassification to bypass config issues.
"""
import torch
import numpy as np
from transformers import BertTokenizer, BertForSequenceClassification
from typing import List
from utils.logger import setup_logger

logger = setup_logger("SentimentAnalyzer", "logs/news.log")

class SentimentAnalyzer:
    def __init__(self):
        self.model_name = "yiyanghkust/finbert-tone"
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        
        logger.info(f"Loading FinBERT on {self.device}...")
        # Force slow tokenizer and explicit model class
        self.tokenizer = BertTokenizer.from_pretrained(self.model_name)
        self.model = BertForSequenceClassification.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()
        logger.info("FinBERT loaded successfully.")

    def analyze_single(self, text: str) -> float:
        """
        Analyzes a single headline and returns a sentiment score between -1 and 1.
        """
        if not text or len(text) < 5:
            return 0.0

        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            probabilities = torch.softmax(logits, dim=1)
            
            # FinBERT outputs: [Negative, Neutral, Positive]
            neg_score = probabilities[0][0].item()
            neu_score = probabilities[0][1].item()
            pos_score = probabilities[0][2].item()

        sentiment_score = (pos_score * 1) + (neu_score * 0) + (neg_score * -1)
        return round(sentiment_score, 4)

    def analyze_batch(self, headlines: List[str]) -> List[float]:
        scores = []
        for headline in headlines:
            scores.append(self.analyze_single(headline))
        return scores

    def get_aggregate_sentiment(self, headlines: List[str]) -> float:
        if not headlines:
            return 0.0
        
        scores = self.analyze_batch(headlines)
        avg_score = np.mean(scores)
        avg_score = max(-1.0, min(1.0, avg_score))
        
        logger.info(f"Aggregate sentiment score: {avg_score:.4f} based on {len(headlines)} headlines.")
        return round(avg_score, 4)