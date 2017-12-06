import json
import Datalog_Parsing as dp;

class Mediator(object):

	def __init__(self):       
		with open('metadictionary.json', 'r') as f:
			 metainfo = json.load(f)
		self.mapping_dict = metainfo

	def queryMediator(self, datalog):
		'''function to return dataframe corresponding to datalog query over mediated schema'''
		#pass_to_QPE = self.unfold_datalog(datalog)
		#return_to_ML = QPE_function(pass_to_QPE) # need function name from query team
		return self.unfold_datalog(datalog) # will eventually return dataframe or equivalent
		
	def validateDatalogAtoms(self, datalog):
		'''function to validate that mediated schema relations used in datalog body have correct number of atoms'''
		try:
			processed = dp.processDatalog(datalog)
		except:
			raise Exception('Error processing datalog')
		
		for idx,parts in enumerate(processed['single_parts']):
			for body in parts['body']:
				parsed_dl = dp.parsePredicateAtoms(body)
				pred = parsed_dl['predicate']
				if pred in self.mapping_dict.keys():
					actual_num_atoms = len(parsed_dl['atoms'])
					expected_num_atoms = len(self.mapping_dict[pred]['datalog'])
					if actual_num_atoms != expected_num_atoms:
						err_string = 'wrong number of atoms in {}'.format(pred)
						raise Exception(err_string)
					else:
						pass

	def validateDatalogConds(self, datalog):
		'''function to validate that datalog condition attributes appear somewhere in the body'''
		try:
			processed = dp.processDatalog(datalog)
		except:
			raise Exception('Error processing datalog')

		cond_attr_set = set()
		body_attr_set = set()
		
		for idx,parts in enumerate(processed['single_parts']):
			bp = parts['body.parsed']
			gb = parts['groupby.parsed']
			ob = parts['orderby.parsed']
			tn = parts['topn.parsed']
			cp = parts['conditions.parsed']
			
			# add body attributes
			if bp is not None and bp is not []:
				for b in bp:
					for b_attr in b['atoms']:
						body_attr_set.add(b_attr)

			# add group by attributes and aggregate alias
			if gb is not None and gb is not []:
					for gb_attr in gb['predicate']['atoms']:
						body_attr_set.add(gb_attr)
					body_attr_set.add(gb['alias'])

			# add order by attributes
			if ob is not None and ob is not []:
				for ob_attr in ob['atoms']:
					body_attr_set.add(ob_attr)
			
			# add top n attributes
			if tn is not None and tn is not []:
				for tn_attr in tn['query']['atoms']:
					body_attr_set.add(tn_attr)

			# extract condition attributes
			if cp is not None and cp is not []:
				for c in cp:
					c_attr = c['condition']['lhs']
					cond_attr_set.add(c_attr)

		# remove underscores from body attributes
		if '_' in body_attr_set:
			body_attr_set.remove('_')

		# check that cond attributes appear in set of body attributes
		for cond_attr in cond_attr_set:
			if cond_attr in body_attr_set:
				continue
			else:
				err_string = 'condition attribute {} is not in datalog body'.format(cond_attr)
				raise Exception(err_string)

		# error if any condition attributes not in body attributes
		if len(cond_attr_set.difference(body_attr_set)) > 0:
			err_string = 'condition attribute not in datalog body'
			raise Exception(err_string)

	def validateDatalogHead(self, datalog):
		'''function to validate that datalog head attributes appear somewhere in the body'''
		try:
			processed = dp.processDatalog(datalog)
		except:
			raise Exception('Error processing datalog')
		
		# collect set of body attributes
		body_attr_set = set()
		for idx,parts in enumerate(processed['single_parts']):
			bp = parts['body.parsed']
			gb = parts['groupby.parsed']
			ob = parts['orderby.parsed']
			tn = parts['topn.parsed']
			
			# add body attributes
			if bp is not None and bp is not []:
				for b in bp:
					for b_attr in b['atoms']:
						body_attr_set.add(b_attr)

			# add group by attributes and aggregate alias
			if gb is not None and gb is not []:
					for gb_attr in gb['predicate']['atoms']:
						body_attr_set.add(gb_attr)
					body_attr_set.add(gb['alias'])

			# add order by attributes
			if ob is not None and ob is not []:
				for ob_attr in ob['atoms']:
					body_attr_set.add(ob_attr)
			
			# add top n attributes
			if tn is not None and tn is not []:
				for tn_attr in tn['query']['atoms']:
					body_attr_set.add(tn_attr)
					
			# check that head attributes appear in set of body attributes
			for head_attr in parts['head.parsed']['atoms']:
				if head_attr in body_attr_set:
					continue
				else:
					err_string = 'head attribute {} is not in datalog body'.format(head_attr)
					raise Exception(err_string)

	def extract_aggreg_subgoals_as_intermediate_steps(self, datalog):
		'''function to turn mediated schema predicates within orderby, groupby, topn into intermediate datalog steps'''
		processed = dp.processDatalog(datalog)
		processed_intermed_step = dp.processDatalog(datalog)
		datalog_strings = []

		for idx,part in enumerate(processed['single_parts']):
			# create new part to overwrite with updates as needed
			new_part = {}
			new_part['head'] = part['head']
			new_part['head.parsed'] = part['head.parsed']
			new_part['body'] = part['body']
			new_part['body.parsed'] = part['body.parsed']
			new_part['groupby'] = part['groupby']
			new_part['groupby.parsed'] = part['groupby.parsed']
			new_part['orderby'] = part['orderby']
			new_part['orderby.parsed'] = part['orderby.parsed']
			new_part['topn'] = part['topn']
			new_part['topn.parsed'] = part['topn.parsed']
			new_part['conditions'] = part['conditions']
			new_part['conditions.parsed'] = part['conditions.parsed']

			# extract aggregate info
			gb_parsed = part['groupby.parsed']
			ob_parsed = part['orderby.parsed']
			topn_parsed = part['topn.parsed']

			# groupby
			if gb_parsed is not None and gb_parsed is not []:
				gb_subgoal = part['groupby.parsed']['predicate']
				if gb_subgoal['predicate'] in self.mapping_dict.keys():
					# build datalog for new intermediate step
					gb_pred = gb_subgoal['predicate']
					gb_atoms = gb_subgoal['atoms']
					intermed_step_body = gb_pred + '(' + ','.join(gb_atoms) + ')'
					intermed_step_head = gb_pred + '_groupby_'+str(idx+1)+'(' + ','.join(gb_atoms) + ')'
					intermed_step_datalog = intermed_step_head + ':-' + intermed_step_body

					# update step - replace ms_relation subgoal with head of intermediate step
					new_gb_subgoal = intermed_step_head
					new_gb_vars = '['+','.join(gb_parsed['by'])+']'
					new_gb_func = '{}={}({})'.format(gb_parsed['alias'],gb_parsed['function'],gb_parsed['on'])
					new_gb_str = 'groupby({},{},{})'.format(new_gb_subgoal, new_gb_vars, new_gb_func)

					# update new_part
					new_part['groupby'] = [new_gb_str]

					# add string to datalog strings
					datalog_str_ext = [intermed_step_datalog, dp.buildDatalogString(new_part)]
					datalog_strings.extend(datalog_str_ext)
				else: # passthrough
					datalog_str_ext = [dp.buildDatalogString(part)]
					datalog_strings.extend(datalog_str_ext)
				continue

			# orderby
			elif ob_parsed is not None and ob_parsed is not []:
				if ob_parsed['predicate'] in self.mapping_dict.keys():
					# build datalog for new intermediate step
					ob_pred = ob_parsed['predicate']
					ob_atoms = ob_parsed['atoms']
					intermed_step_body = ob_pred + '(' + ','.join(ob_atoms) + ')'
					intermed_step_head = ob_pred + '_orderby_'+str(idx+1)+'(' + ','.join(ob_atoms) + ')'
					intermed_step_datalog = intermed_step_head + ':-' + intermed_step_body

					# update step - replace ms_relation subgoal with head of intermediate step
					new_ob_subgoal = intermed_step_head
					new_ob_vars = '['+','.join([v[0] for v in ob_parsed['by']])+']'
					new_ob_sort = '['+','.join([v[1] for v in ob_parsed['by']])+']'
					new_ob_str = 'orderby({},{},{})'.format(new_ob_subgoal, new_ob_vars, new_ob_sort)

					# update new part
					new_part['orderby'] = [new_ob_str]

					# add string to datalog strings
					datalog_str_ext = [intermed_step_datalog, dp.buildDatalogString(new_part)]
					datalog_strings.extend(datalog_str_ext)
				else: # passthrough
					datalog_str_ext = [dp.buildDatalogString(part)]
					datalog_strings.extend(datalog_str_ext)
				continue

			# topn
			elif topn_parsed is not None and topn_parsed is not []:
				if topn_parsed['query']['predicate'] in self.mapping_dict.keys():
					# build datalog for new intermediate step
					tn_pred = topn_parsed['query']['predicate']
					tn_atoms = topn_parsed['query']['atoms']
					intermed_step_body = tn_pred + '(' + ','.join(tn_atoms) + ')'
					intermed_step_head = tn_pred + '_topn_'+str(idx+1)+'(' + ','.join(tn_atoms) + ')'
					intermed_step_datalog = intermed_step_head + ':-' + intermed_step_body

					# update step - replace ms_relation subgoal with head of intermediate step
					new_tn_subgoal = intermed_step_head
					new_tn_str = 'top({},{})'.format(topn_parsed['n'], new_tn_subgoal)

					# update new part
					new_part['topn'] = [new_tn_str]

					# add string to datalog strings
					datalog_str_ext = [intermed_step_datalog, dp.buildDatalogString(new_part)]
					datalog_strings.extend(datalog_str_ext)
				else: # passthrough
					datalog_str_ext = [dp.buildDatalogString(part)]
					datalog_strings.extend(datalog_str_ext)
				continue

			else: # passthrough
				datalog_strings.extend([dp.buildDatalogString(part)])

		datalog_result = '.'.join(datalog_strings)#.lower()
		return datalog_result

	def optimize_datalog(self, datalog_dict):
		'''function to remove unnecessary relations from dictionary of datalog relations and atoms'''
		opt_dict = {}

		# loop through tables
		for idx1,k1 in enumerate(datalog_dict.keys()):
			k1_attr_set = set(datalog_dict[k1])

			# create set of attributes appearing in tables other than k1
			non_k1_attr_set = set()
			for idx2,k2 in enumerate(datalog_dict.keys()):
				if idx1 == idx2:
					continue
				else:
					for a in datalog_dict[k2]:
						non_k1_attr_set.add(a)

			# remove underscores
			if '_' in k1_attr_set:
				k1_attr_set.remove('_')
			if '_' in non_k1_attr_set:
				non_k1_attr_set.remove('_')

			# add relation k1 to optmized dictionary if k1 has unique atoms
			if len(k1_attr_set.difference(non_k1_attr_set)) > 0:
				opt_dict[k1] = datalog_dict[k1]
				
		return opt_dict

	def unfold_datalog_body(self, datalog):
		'''function to unfold datalog string (predicate and atoms) according to mapping dictionary'''
		parsed_dl = dp.parsePredicateAtoms(datalog)
		pred = parsed_dl['predicate']
		
		if pred in self.mapping_dict.keys():
			# create mapping dictionary
			mapping_dict = {}
			for idx,d in enumerate(self.mapping_dict[pred]['datalog']):
				mapping_dict[d] = parsed_dl['atoms'][idx]

			# create source dictionary
			src_dict = {}

			for idx,m in enumerate(self.mapping_dict[pred]['mapping']):
				src = m['source']
				src_tbl = m['table']
				src_dl = m['source.datalog']

				res_dl = ['_'] * len(src_dl)

				for idx,s in enumerate(src_dl):
					res_dl[idx] = mapping_dict[s]

				src_dict[src+'.'+src_tbl] = res_dl
	   
			# optimize to eliminate unneeded relations
			opt_dict = self.optimize_datalog(src_dict)
		
			# convert filled source dictionary to datalog
			mapped = ','.join([k+'('+','.join(opt_dict[k])+')' for k in sorted(opt_dict.keys())])
		else:
			mapped = datalog
			
		return mapped

	def unfold_datalog(self, datalog):
		'''function to unfold datalog string according to mapping dictionary'''		
		# validate datalog
		for datalog_string in dp.processDatalog(datalog)['single_strings']:
			self.validateDatalogAtoms(datalog_string)
			self.validateDatalogHead(datalog_string)
			self.validateDatalogConds(datalog_string)
		
		# extract aggregate subgoals to be unfolded in intermediate steps
		datalog = self.extract_aggreg_subgoals_as_intermediate_steps(datalog)
		
		# process datalog to extract individual parsed parts
		processed = dp.processDatalog(datalog)
		processed_unfolded = dp.processDatalog(datalog)
		unfolded_datalog_strings = []

		# unfold appropriate body relations
		for idx,parts in enumerate(processed['single_parts']):
			body_unfolded_list = []
			for body in parts['body']:
				body_unfolded = self.unfold_datalog_body(body)
				body_unfolded_list.extend([body_unfolded])

			processed_unfolded['single_parts'][idx]['body'] = body_unfolded_list
			unfolded_datalog_strings.extend([dp.buildDatalogString(processed_unfolded['single_parts'][idx])])

		# create and return final unfolded datalog string and processed object
		unfolded_datalog = '.'.join(unfolded_datalog_strings)
		processed_unfolded = dp.processDatalog(unfolded_datalog)
		return unfolded_datalog, processed_unfolded