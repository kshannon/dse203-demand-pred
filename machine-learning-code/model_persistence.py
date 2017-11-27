# model persistence

import pickle

def persist_model(model_name,super_cat_name,date,model):
	"""
	model and super category name do not have blank spaces
	date in the form: yyyymmdd
	model is a valid scikit learn class instance
	"""

	underscore = '_'
	seq = (model_name,super_cat_name,date)
	filename = underscore.join([str(x) for x in seq]).replace(" ","")

	pkl_model = open(filename, 'wb')
	pickle.dump(model, pkl_model)
	pkl_model.close()



