

def calc_embedding(model, tokenizer, _text):
    encoded_input = tokenizer(_text, padding=True, truncation=True, return_tensors='pt')
    with torch.no_grad():
        model_output = model(**encoded_input)
        # Perform pooling. In this case, cls pooling.
        sentence_embeddings = model_output[0][:, 0]
    return sentence_embeddings
    # normalize embeddings
