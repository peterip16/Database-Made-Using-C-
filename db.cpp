/*********************7***************************************
	Project#1:	CLP & DDL
 ************************************************************/
#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

 //table file header struct
typedef struct table_file_header_def
{
	int			file_size;			// 4 bytes
	int			record_size;			// 4 bytes
	int			num_records;			// 4 bytes
	int			record_offset;			// 4 bytes
	int			file_header_flag;		// 4 bytes
	tpd_entry		*tpd_ptr;			// 4 bytes
} table_file_header;

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;


	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}
	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
				tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr = tmp_tok_ptr;
		}
	}

	return rc;
}


/*************************************************************
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc = 0, i, j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	//printf("%s", command);

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset((void*)temp_string, '\0', MAX_TOK_LEN); //sets everything to null
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do
			{
				temp_string[i++] = *cur++;
			} while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET + j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET + j >= K_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET + j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do
			{
				temp_string[i++] = *cur++;
			} while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
			|| (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(': t_value = S_LEFT_PAREN; break;
			case ')': t_value = S_RIGHT_PAREN; break;
			case ',': t_value = S_COMMA; break;
			case '*': t_value = S_STAR; break;
			case '=': t_value = S_EQUAL; break;
			case '<': t_value = S_LESS; break;
			case '>': t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do
			{
				temp_string[i++] = *cur++;
			} while ((*cur) && (*cur != '\''));  //keep moving the cursor forward and adding the characters to temp_string as long as the next characters exist (not null)

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
		((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_SELECT)
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DELETE) && ((cur->next != NULL)))
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_UPDATE)) {
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) && (cur->next->tok_value == K_TO))
	{
		printf("BACKUP statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch (cur_cmd)
		{
		case CREATE_TABLE:
			rc = sem_create_table(cur);
			break;
		case DROP_TABLE:
			rc = sem_drop_table(cur);
			break;
		case LIST_TABLE:
			rc = sem_list_tables();
			break;
		case LIST_SCHEMA:
			rc = sem_list_schema(cur);
			break;
		case INSERT:
			rc = sem_insert(cur);
			break;
		case SELECT:
			rc = sem_select(cur);
			break;
		case DELETE:
			rc = sem_delete(cur);
			break;
		case UPDATE:
			rc = sem_update(cur);
			break;
		case BACKUP:
			rc = sem_backup(cur);
			break;
		default:
			; /* no action */
		}
	}

	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];
	FILE * f;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;

	memset(filename, '\0', MAX_IDENT_LEN + 1);
	memset(tab_name, '\0', MAX_IDENT_LEN + 1);
	strcpy(tab_name, cur->tok_string);
	strcpy(filename, cur->tok_string);
	strcat(filename, ".tab");

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for (i = 0; i < cur_id; i++)
						{
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string) == 0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
								/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);

										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}
										else if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value == K_NULL))
										{
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value == K_NULL))
													{
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							(void*)&tab_entry,
							sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
							(void*)col_entry,
							sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);


						table_file_header* newTable = NULL;
						newTable = (table_file_header*)calloc(1, sizeof(table_file_header));
						memset(newTable, '\0', sizeof(newTable));

						//Get record size of table file header
						int bytes = 0;
						for (int i = 0; i < tab_entry.num_columns; i++)
						{
							if (col_entry[i].col_type == T_CHAR)
							{
								bytes += (1 + col_entry[i].col_len);
							}
							else
							{
								bytes += 1 + 4;
							}
						}
						if (bytes % 4 != 0)
						{
							bytes += (4 - (bytes % 4));
						}
						newTable->file_size = sizeof(table_file_header);
						newTable->record_size = bytes;
						newTable->num_records = 0;
						newTable->record_offset = sizeof(table_file_header);
						newTable->tpd_ptr = 0;
						f = fopen(filename, "w");
						fwrite(newTable, sizeof(table_file_header), 1, f);
						fflush(f);
						fclose(f);
						free(newTable);
						free(new_entry);
					}
				}
			}
		}
	}
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];

	cur = t_list;

	memset(filename, '\0', MAX_IDENT_LEN + 1);
	memset(tab_name, '\0', MAX_IDENT_LEN + 1);
	strcpy(tab_name, cur->tok_string);
	strcpy(filename, cur->tok_string);
	strcat(filename, ".tab");

	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}
	// Delete the .tab file
	if (remove(filename) == 0)
	{
		printf("File %s has been successfully removed\n", tab_name);
	}
	else
	{
		printf("Unable to delete the file\n");
		perror("Error");
	}

	return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur = t_list;
	token_list *temp_cur = NULL;
	cd_entry  *col_entry;
	tpd_entry *tab_entry;
	row *insertedRow = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	FILE *f = NULL;
	struct _stat file_stat;
	table_file_header *file_header = NULL; //current file_header
	table_file_header *new_file_header = NULL; //new file_header
	int offset = 0;
	int newSize; //hold new record size
	int tempSize; //temp hold for record size
	int i;

	//check if the table exist
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID_TABLE_NAME;
	}
	else
	{
		cur = cur->next;
		//Check if 'VALUES' comes next
		if (cur->tok_value != K_VALUES)
		{
			printf("'VALUES' missing from statement\n");
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			//Check for '(' after VALUES
			if (cur->tok_value != S_LEFT_PAREN)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{
				strcpy(filename, tab_entry->table_name);
				strcat(filename, ".tab");
				if ((f = fopen(filename, "rbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					//read everything including the rows
					_fstat(_fileno(f), &file_stat);
					file_header = (table_file_header*)calloc(1, file_stat.st_size);
					fread(file_header, file_stat.st_size, 1, f);
					insertedRow = (row*)calloc(1, file_header->record_size);
					fclose(f);
				}
			}
		}
	}
	if (!rc)
	{
		//Check values being inserted
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
		{
			cur = cur->next;
			//can only be string, int or NULL.
			if ((cur->tok_value != STRING_LITERAL) &&
				(cur->tok_value != INT_LITERAL) &&
				(cur->tok_value != K_NULL))
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{
				//NULL
				if (cur->tok_value == K_NULL)
				{
					if (col_entry->not_null != 0)
					{
						printf("Not Null constraint exists for column: %s\n", col_entry->col_name);
						rc = NOT_NULL_CONSTRAINT;
						cur->tok_value = INVALID;
					}
				}
				//INT
				else if (cur->tok_value == INT_LITERAL)
				{

					if (col_entry->col_type == STRING_LITERAL)
					{
						printf("Type mismatch error\n");
						rc = MISMATCH_TYPE;
						cur->tok_value = INVALID;
					}
					else
					{
						tempSize = sizeof(int);
						int temp_int = atoi(cur->tok_string);
						memcpy(insertedRow + offset, &tempSize, 1);
						memcpy(insertedRow + offset + 1, &temp_int, tempSize);
					}

				}
				//STRING
				else if (cur->tok_value == STRING_LITERAL)
				{
					if (col_entry->col_type == T_INT)
					{
						printf("Type mismatch error\n");
						rc = MISMATCH_TYPE;
						cur->tok_value = INVALID;
					}
					else
					{
						tempSize = strlen(cur->tok_string);
						memcpy(insertedRow + offset, &tempSize, 1);
						memcpy(insertedRow + offset + 1, cur->tok_string, tempSize);
					}
				}

				//increase the offset
				offset += (1 + col_entry->col_len);

				//check for the comma
				if (cur->next->tok_value != S_COMMA)
				{
					if (!(i == tab_entry->num_columns - 1))
					{
						rc = INVALID_STATEMENT;
						printf("Comma needed after each value (except for last)\n");
						cur->next->tok_value = INVALID;
					}
				}
				else
				{
					cur = cur->next;
				}
			}
		}
	}
	//If no errors, write to the file
	if (!rc)
	{
		if (file_header->num_records == MAX_ROWS)
		{
			//already at max # of rows allowed
			printf("Max number of rows reached, pls stop trying to add rows here");
		}
		else
		{
			newSize = file_header->file_size + file_header->record_size;
			new_file_header = (table_file_header*)calloc(1, newSize);
			memcpy(new_file_header, file_header, file_header->file_size);
			memcpy((void*)((char*)new_file_header + file_header->file_size), (void*)insertedRow, file_header->record_size);

			//update file_header values
			new_file_header->file_size = newSize;
			new_file_header->num_records++;

			if ((f = fopen(filename, "wbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				fwrite(new_file_header, new_file_header->file_size, 1, f);
				printf("Size of %s: %i. Number of records: %i\n", filename, new_file_header->file_size, new_file_header->num_records);
			}

		}
	}

	return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0;
	token_list *cur = t_list;
	tpd_entry *tab_entry;
	cd_entry  *col_entry;
	cd_entry temp_col_entry[1000];
	where_conds *where_cons = NULL;
	row *selectedRow = NULL;
	row *row_offset = NULL;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	table_file_header *file_header = NULL;
	char filename[MAX_IDENT_LEN + 1];
	char extra[9999];
	char column_string[9999];
	char tempString[MAX_IDENT_LEN + 1];
	char* orderbycolumn = (char*)malloc(MAX_IDENT_LEN + 1);
	int temp_int;
	int temp_size;
	int offset = 0;
	bool con_check = false;
	bool where_exist = true;
	bool avg_flag = false;
	bool sum_flag = false;
	bool count_flag = false;
	bool order_by_flag = false;
	bool desc_flag = false;

	//printf("The string currently cursor is pointing to is: %s\n", cur->tok_string);

	if (cur->tok_value == S_STAR)
	{
		cur = cur->next;
		if (cur->tok_value != K_FROM)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID_TABLE_NAME;
			}
			else
			{
				if (cur->next->tok_value == K_WHERE || cur->next->tok_value == EOC || cur->next->tok_value == K_ORDER)
				{
					//printf("Where we split up...\n");
					if (cur->next->tok_value == K_WHERE) {
						//printf("Where statement detected...\n");
						//where_exist = true;
						where_cons = (where_conds*)malloc(sizeof(where_conds));
						where_cons->next = NULL;
						cur = cur->next;
						if (cur->next->tok_value == IDENT) {
							if (cur->next->next->tok_value == S_EQUAL || cur->next->next->tok_value == S_LESS || cur->next->next->tok_value == S_GREATER) {
								if (cur->next->next->next->tok_value == STRING_LITERAL || cur->next->next->next->tok_value == INT_LITERAL) {

									where_cons->col_name = cur->next->tok_string;
									where_cons->op_tok_value = cur->next->next->tok_value;
									where_cons->val = cur->next->next->next->tok_string;
									where_cons->val_tok_value = cur->next->next->next->tok_value;
									cur = cur->next->next->next;

									where_conds *temp_where_cons = where_cons;

									while ((cur->next->tok_value == K_AND || cur->next->tok_value == K_OR) && !rc) {

										temp_where_cons->next = (where_conds*)malloc(sizeof(where_conds));
										temp_where_cons = temp_where_cons->next;
										temp_where_cons->next = NULL;
										cur = cur->next;

										if (cur->next->tok_value == IDENT) {
											if (cur->next->next->tok_value == S_EQUAL || cur->next->next->tok_value == S_LESS || cur->next->next->tok_value == S_GREATER) {
												if (cur->next->next->next->tok_value == STRING_LITERAL || cur->next->next->next->tok_value == INT_LITERAL) {
													temp_where_cons->col_name = cur->next->tok_string;
													temp_where_cons->op_tok_value = cur->next->next->tok_value;
													temp_where_cons->val = cur->next->next->next->tok_string;
													temp_where_cons->val_tok_value = cur->next->next->next->tok_value;
													cur = cur->next->next->next;
												}
												else {
													rc = INVALID_STATEMENT;
												}
											}
											else {
												rc = INVALID_STATEMENT;
											}
										}
										else {
											rc = INVALID_STATEMENT;
										}
									}

								}
								else {
									rc = INVALID_STATEMENT;
								}
							}
							else {
								rc = INVALID_STATEMENT;
							}
						}
					}
					else {
						where_exist = false;
					}

					printf("%s", cur->next->tok_string);

					if (cur->next->tok_value == K_ORDER) {
						//printf("Entering order by loop...\n");
						cur = cur->next;
						if (cur->next->tok_value == K_BY) {
							cur = cur->next;
							if (cur->next->tok_value == IDENT) {
								order_by_flag = true;
								orderbycolumn = cur->next->tok_string;
								//	printf("Identified column name%s\n", orderbycolumn);
								cur = cur->next;
								if (cur->next->tok_value == K_DESC) {
									printf("Desc flag raised\n");
									desc_flag = true;
								}
							}
							else {
								rc = INVALID_STATEMENT;
							}
						}
						else {
							rc = INVALID_STATEMENT;
						}
					}
					else {
						rc = INVALID_STATEMENT;
					}

					//read from file and print
					strcpy(filename, tab_entry->table_name);
					strcat(filename, ".tab");
					if ((fhandle = fopen(filename, "rbc")) == NULL)
					{
						rc = FILE_OPEN_ERROR;
					}
					else
					{
						//printf("981");
						_fstat(_fileno(fhandle), &file_stat);
						file_header = (table_file_header*)calloc(1, file_stat.st_size);
						fread(file_header, file_stat.st_size, 1, fhandle);
						fclose(fhandle);
						//read other rows here
						int i;

						row_datas* records = (row_datas*)malloc(sizeof(row_datas));
						records->next_col = NULL;
						records->next_row = NULL;
						bool has_value = false;

						for (i = 0; i < tab_entry->num_columns; i++)
						{
							strcat(extra, "+----------------");
						}
						strcat(extra, "+");
						printf("%s\n", extra);

						for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("|%-16s", col_entry->col_name);
						}
						int row_match = 0;
						printf("|\n");
						printf("%s\n", extra);
						int j;
						row_datas* rowhead = (row_datas*)malloc(sizeof(row_datas));
						rowhead = records;

						for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < file_header->num_records; i++, selectedRow += file_header->record_size)
						{
							offset = 0;

							if (where_cons != NULL) {
								//printf("1260\n");
								con_check = check_condition(tab_entry, selectedRow, where_cons);
							}
							if (con_check || !where_exist) {
								row_match++;
								row_datas* columnhead = (row_datas*)malloc(sizeof(row_datas));
								columnhead = rowhead;

								for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); j < tab_entry->num_columns; j++, col_entry++)
								{

									//printf("Offset is : %d\n", offset);
									memcpy(&temp_size, selectedRow + offset, 1);
									offset++;
									//NULL
									if (temp_size == 0)
									{
										printf("|%-16s", "NULL");
									}
									//INT
									else if (col_entry->col_type == T_INT)
									{
										memset((&temp_int), ('\0'), (col_entry->col_len));
										memcpy((&temp_int), (selectedRow + offset), (temp_size));
										printf("|%-16i", temp_int);
										columnhead->has_value = true;
										columnhead->tok_value = INT_LITERAL;
										columnhead->int_val = temp_int;
									}
									//STRING
									else
									{
										memset(tempString, '\0', MAX_IDENT_LEN + 1);
										memcpy(tempString, selectedRow + offset, col_entry->col_len);
										printf("|%-16s", tempString);
										columnhead->has_value = true;
										columnhead->tok_value = STRING_LITERAL;
										columnhead->string_val = (char*)tempString;
									}

									//printf(" Record holds: %s\n", records->string_val); //Used for checking order by values. Must check back here.

									columnhead->next_col = (row_datas*)malloc(sizeof(row_datas));
									columnhead = columnhead->next_col;
									columnhead->has_value = false;
									columnhead->next_col = NULL;
									columnhead->next_row = NULL;
									offset += col_entry->col_len;
									//printf("Coleumn length is: %d\n", col_entry->col_len);

								}
								printf("|\n");
								if (i != file_header->num_records - 1) {
									rowhead->next_row = (row_datas*)malloc(sizeof(row_datas));
									rowhead = (row_datas*)(rowhead->next_row);
									rowhead->has_value = false;
									rowhead->next_col = NULL;
									rowhead->next_row = NULL;
								}
							}

						}
						printf("%s\n", extra);
						//printf("%i rows selected.\n", file_header->num_records);

						printf("%i rows selected.\n", row_match);
						/*
						row_datas* temp_row_records = NULL;
						*/

					}
				}
			}// where the statements after table is concluded
		}
	}
	else if ((cur->tok_class != keyword) && (cur->tok_class != type_name) && (cur->tok_class != function_name))// For normally selecting columns and stuff
	{
		//printf("Column names\n");
		char *selected_columns[] = { "" };
		int cur_id = 0;
		int column_match;
		bool done = false;
		//cur = cur->next;

		do
		{
			if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name) && (cur->tok_value != K_FROM))
			{
				// Error
				//printf("1055 %d\n", cur->tok_class);
				rc = INVALID_COLUMN_NAME;
				cur->tok_value = INVALID;
			}
			else if (cur->tok_value == EOC) {
				//printf("1060");
				rc = INVALID_COLUMN_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				//printf("1066");
				strcpy(temp_col_entry[cur_id].col_name, cur->tok_string);
				temp_col_entry[cur_id].col_id = cur_id;
				temp_col_entry[cur_id].not_null = false;    /* set default */
															//printf("1070");

				cur = cur->next;
				cur_id++;

				if (cur->tok_value != S_COMMA) {
					done = true;
				}

				if ((!rc) && (!done))
				{
					//cur_id++;
					cur = cur->next;
				}
			}
		} while ((rc == 0) && (cur->tok_value != K_FROM) && !done);

		//printf("Congratulations!");


		//save column names, must be strings.  if not, give bad return code
		//printf("%d\n", cur->tok_value);
		if (cur->tok_value != K_FROM)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			//printf("Checking if this is an actual table...\n");
			cur = cur->next;
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID_TABLE_NAME;
				//printf("1096");
			}
			else
			{
				//read from file and print
				//printf("It is an actual table. Current String is: %s\n", cur->tok_string);
				if (cur->next->tok_value == EOC || cur->next->tok_value == K_WHERE || cur->next->tok_value == K_ORDER) {
					if (cur->next->tok_value == K_WHERE) {
						//printf("Where statement detected...\n");
						//where_exist = true;
						where_cons = (where_conds*)malloc(sizeof(where_conds));
						where_cons->next = NULL;
						cur = cur->next;
						if (cur->next->tok_value == IDENT) {
							if (cur->next->next->tok_value == S_EQUAL || cur->next->next->tok_value == S_LESS || cur->next->next->tok_value == S_GREATER) {
								if (cur->next->next->next->tok_value == STRING_LITERAL || cur->next->next->next->tok_value == INT_LITERAL) {

									where_cons->col_name = cur->next->tok_string;
									where_cons->op_tok_value = cur->next->next->tok_value;
									where_cons->val = cur->next->next->next->tok_string;
									where_cons->val_tok_value = cur->next->next->next->tok_value;
									cur = cur->next->next->next;

									where_conds *temp_where_cons = where_cons;

									while ((cur->next->tok_value == K_AND || cur->next->tok_value == K_OR) && !rc) {

										temp_where_cons->next = (where_conds*)malloc(sizeof(where_conds));
										temp_where_cons = temp_where_cons->next;
										temp_where_cons->next = NULL;
										cur = cur->next;

										if (cur->next->tok_value == IDENT) {
											if (cur->next->next->tok_value == S_EQUAL || cur->next->next->tok_value == S_LESS || cur->next->next->tok_value == S_GREATER) {
												if (cur->next->next->next->tok_value == STRING_LITERAL || cur->next->next->next->tok_value == INT_LITERAL) {
													temp_where_cons->col_name = cur->next->tok_string;
													temp_where_cons->op_tok_value = cur->next->next->tok_value;
													temp_where_cons->val = cur->next->next->next->tok_string;
													temp_where_cons->val_tok_value = cur->next->next->next->tok_value;
													cur = cur->next->next->next;
												}
												else {
													rc = INVALID_STATEMENT;
												}
											}
											else {
												rc = INVALID_STATEMENT;
											}
										}
										else {
											rc = INVALID_STATEMENT;
										}
									}

								}
								else {
									rc = INVALID_STATEMENT;
								}
							}
							else {
								rc = INVALID_STATEMENT;
							}
						}
					}
					else {
						where_exist = false;
						//printf("It shouldn't come here...");
					}

					//printf("%d\n", where_exist);

					if (cur->next->tok_value == K_ORDER) {
						cur = cur->next;
						if (cur->next->tok_value == K_BY) {
							cur = cur->next;
							if (cur->next->tok_value == IDENT) {
								order_by_flag = true;
								orderbycolumn = cur->next->tok_string;
								cur = cur->next;
								if (cur->next->tok_value == K_DESC) {
									desc_flag = true;
								}
							}
							else {
								rc = INVALID_STATEMENT;
							}
						}
						else {
							rc = INVALID_STATEMENT;
						}
					}
					else {
						rc = INVALID_STATEMENT;
					}

					strcpy(filename, tab_entry->table_name);
					strcat(filename, ".tab");
					if ((fhandle = fopen(filename, "rbc")) == NULL)
					{
						rc = FILE_OPEN_ERROR;
					}
					else if (!rc)
					{
						//check to see if columns exist in the table
						_fstat(_fileno(fhandle), &file_stat);
						file_header = (table_file_header*)calloc(1, file_stat.st_size);
						fread(file_header, file_stat.st_size, 1, fhandle);
						fclose(fhandle);
						//read other rows here
						int i;

						//for (i = 0; i < tab_entry->num_columns; i++) //less than # of selected columns
						//printf("%d\n", cur_id);
						int extra_len = 0;
						for (i = 0; i < cur_id; i++)
						{
							//strcat(extra, "+----------------");
							//extra_len += snprintf(extra+extra_len, sizeof(extra)-extra_len, "+----------------", col_entry->col_name); //disuse snprintf in case of compiler problem
							//printf("%d\n%s\n", extra_len, extra);
							sprintf(extra + extra_len, "+----------------", extra);
							extra_len += sizeof("+----------------") - 1; // -1 because of null terminator
																		  //printf("%d\n", extra_len);
						}
						//strcat(extra, "+\n");
						//extra_len += snprintf(extra+extra_len, sizeof(extra)-extra_len, "+\n", col_entry->col_name); //disuse snprintf in case of compiler problem
						//printf("%d\n%s\n", extra_len, extra); 
						sprintf(extra + extra_len, "+\n", extra);
						extra_len += sizeof("+\n") - 1; // -1 because of null terminator
														//printf("%d\n", extra_len);

														//printf("%s\n", extra);
														//read through the columns
						column_match = 0;
						//for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++){
						int extra_colemn_len = 0;
						for (int n = 0; n < cur_id; n++) {
							//printf("|%-16s", col_entry->col_name);
							//add the columns to some list to check with entered columns
							//extra_len += snprintf(extra + extra_len, sizeof(extra) - extra_len, "|%-16s", col_entry->col_name); //disuse snprintf in case of compiler problem
							//for (int n = 0; n < cur_id; n++) {
							for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++) {
								char str1[sizeof(col_entry->col_name)]; //trying to store one of the string to a variable for comparison 
								for (int t = 0; t < sizeof(col_entry->col_name); t++) {
									str1[t] = tolower(col_entry->col_name[t]);
								}
								char str2[sizeof(temp_col_entry->col_name)]; //trying to store one of the string to a variable for comparison 
								for (int t = 0; t < sizeof(temp_col_entry[n].col_name); t++) {
									str2[t] = tolower(temp_col_entry[n].col_name[t]);
								}
								if (!stricmp(str1, str2)) { //If They are the same assuming case insensitive, concatenate the column name into it
									sprintf(column_string + extra_colemn_len, "|%-16s", col_entry->col_name);
									extra_colemn_len += sizeof("+----------------") - 1; // -1 because of null terminator
									column_match++;
								}
							}
						}

						///printf("%s|\n", extra);
						sprintf(column_string + extra_colemn_len, "|\n", col_entry->col_name);
						extra_colemn_len += sizeof("|\n") - 1;
						printf("%s%s%s", extra, column_string, extra);

						offset = 0;
						//for (int n = 0; n < cur_id; n++)
						for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < file_header->num_records; i++, selectedRow += file_header->record_size)
						{
							//printf("1254\n");
							for (int n = 0; n < cur_id; n++)
							{
								int t;

								if (where_cons != NULL) {
									//printf("1260\n");
									con_check = check_condition(tab_entry, selectedRow, where_cons);
								}

								//printf("The conditions are: %d %d\n", con_check, !where_exist);

								if (con_check || !where_exist) {
									for (t = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); t < tab_entry->num_columns; t++, col_entry++)
									{
										char str1[sizeof(col_entry->col_name)]; //trying to store one of the string to a variable for comparison
										int str1_len = sizeof(col_entry->col_name);
										int e;
										//for (e = 0; e < sizeof(col_entry->col_name); e++) {
										for (e = 0; e < str1_len; e++) {
											if (isalpha(col_entry->col_name[e])) {
												str1[e] = tolower(col_entry->col_name[e]);
											}
											else {
												str1[e] = (col_entry->col_name[e]);
											}
										}
										//printf("1194 ");
										char str2[sizeof(temp_col_entry->col_name)]; //trying to store one of the string to a variable for comparison 
										int str2_len = sizeof(temp_col_entry[n].col_name);
										//printf("1197 ");
										for (e = 0; e < str2_len; e++) {
											if (isalpha(col_entry->col_name[e])) {
												str2[e] = tolower(temp_col_entry[n].col_name[e]);
											}
											else {
												str2[e] = (col_entry->col_name[e]);
											}
										}
										//printf("1205");
										//if (strcmp(str1, str2) == 0)
										//{
										//printf("%s %s\n", str1, str2);
										memcpy(&temp_size, selectedRow + offset, 1);
										offset++;

										if (strcmp(str1, str2) == 0)
										{
											//NULL
											if (temp_size == 0)
											{
												printf("|%-16s", "NULL");
											}
											//INT
											else if (col_entry->col_type == T_INT)
											{
												memset(&temp_int, '\0', col_entry->col_len);
												memcpy(&temp_int, selectedRow + offset, temp_size);
												printf("|%-16i", temp_int);
											}
											//STRING
											else
											{
												memset(tempString, '\0', MAX_IDENT_LEN + 1);
												memcpy(tempString, selectedRow + offset, col_entry->col_len);
												printf("|%-16s", tempString);
											}
										}
										offset += col_entry->col_len;
									}
									//printf("|\n");
									offset = 0;
								}
								//if (con_check || !where_exist) printf("|\n");
							}
							if (con_check || !where_exist) printf("|\n");
							//printf("1326\n");
						}
						//printf("1328\n");
					}
					//printf("1335\n");
					printf("%s\n", extra);
					//printf("%d", column_match);
				}
				else {
					rc = INVALID_STATEMENT;
				}
			}

		}// From clause ends here
	}
	else if (cur->tok_value == K_AVG || cur->tok_value == K_SUM || cur->tok_value == K_COUNT) {

		char* want_column;
		int extra_column_len = 0;
		int column_match = 0;
		int row_match = 0;
		int sum_match = 0;
		int cur_id = 0;
		bool done = false;
		bool star_flag = false;

		//printf("Now doing aggregate function\n");

		if (cur->tok_value == K_AVG) {
			avg_flag = true;
		}
		else if (cur->tok_value == K_SUM) {
			sum_flag = true;
		}
		else {
			count_flag = true;
		}
		if (cur->next->tok_value == S_LEFT_PAREN) {
			cur = cur->next;
			//printf("1384\n");
			if (cur->next->tok_value == IDENT) {
				//printf("1386\n");
				cur = cur->next;
				want_column = cur->tok_string;
				do
				{
					if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name) && (cur->tok_value != K_FROM))
					{
						// Error
						//printf("1055 %d\n", cur->tok_class);
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else if (cur->tok_value == EOC) {
						//printf("1060");
						rc = INVALID_COLUMN_DEFINITION;
						cur->tok_value = INVALID;
					}
					else
					{
						//printf("1066");
						strcpy(temp_col_entry[cur_id].col_name, cur->tok_string);
						temp_col_entry[cur_id].col_id = cur_id;
						temp_col_entry[cur_id].not_null = false;    /* set default */
						//printf("1070");

						//cur = cur->next;
						cur_id++;

						done = true;

						if ((!rc) && (!done))
						{
							//cur_id++;
							cur = cur->next;
						}
					}
				} while ((rc == 0) && (cur->tok_value != K_FROM) && !done);
			}
			else if (cur->next->tok_value == S_STAR) {
				cur = cur->next;
				//want_column = cur->tok_string;
				star_flag = true;
			}
			else {
				rc = INVALID_STATEMENT;
			}
			if (!rc) {
				//printf("%s\n", cur->next->tok_string);
				if (cur->next->tok_value == S_RIGHT_PAREN) {
					//printf("It is still parsing correctly...\n");
					cur = cur->next;
					if (cur->next->tok_value == K_FROM) {
						//printf("Still correct\n");
						cur = cur->next;
						//printf("%s\n", cur->tok_string);
						if (cur->tok_value != K_FROM)
						{
							printf("1400\n");
							rc = INVALID_STATEMENT;
							cur->tok_value = INVALID;
						}
						else
						{
							//printf("Checking if this is an actual table...\n");
							cur = cur->next;
							if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
							{
								rc = TABLE_NOT_EXIST;
								cur->tok_value = INVALID_TABLE_NAME;
								printf("1411\n");
							}
							else
							{
								//read from file and print
								//printf("It is an actual table. Current String is: %s\n", cur->tok_string);
								if (cur->next->tok_value == EOC || cur->next->tok_value == K_WHERE || cur->next->tok_value == K_ORDER) {
									if (cur->next->tok_value == K_WHERE) {
										//printf("Where statement detected...\n");
										where_exist = true;
										where_cons = (where_conds*)malloc(sizeof(where_conds));
										where_cons->next = NULL;
										cur = cur->next;
										if (cur->next->tok_value == IDENT) {
											if (cur->next->next->tok_value == S_EQUAL || cur->next->next->tok_value == S_LESS || cur->next->next->tok_value == S_GREATER) {
												if (cur->next->next->next->tok_value == STRING_LITERAL || cur->next->next->next->tok_value == INT_LITERAL) {

													where_cons->col_name = cur->next->tok_string;
													where_cons->op_tok_value = cur->next->next->tok_value;
													where_cons->val = cur->next->next->next->tok_string;
													where_cons->val_tok_value = cur->next->next->next->tok_value;
													cur = cur->next->next->next;

													where_conds *temp_where_cons = where_cons;

													while ((cur->next->tok_value == K_AND || cur->next->tok_value == K_OR) && !rc) {

														temp_where_cons->next = (where_conds*)malloc(sizeof(where_conds));
														temp_where_cons = temp_where_cons->next;
														temp_where_cons->next = NULL;
														cur = cur->next;

														if (cur->next->tok_value == IDENT) {
															if (cur->next->next->tok_value == S_EQUAL || cur->next->next->tok_value == S_LESS || cur->next->next->tok_value == S_GREATER) {
																if (cur->next->next->next->tok_value == STRING_LITERAL || cur->next->next->next->tok_value == INT_LITERAL) {
																	temp_where_cons->col_name = cur->next->tok_string;
																	temp_where_cons->op_tok_value = cur->next->next->tok_value;
																	temp_where_cons->val = cur->next->next->next->tok_string;
																	temp_where_cons->val_tok_value = cur->next->next->next->tok_value;
																	cur = cur->next->next->next;
																}
																else {
																	printf("1453\n");
																	rc = INVALID_STATEMENT;
																}
															}
															else {
																printf("1458\n");
																rc = INVALID_STATEMENT;
															}
														}
														else {
															printf("1463\n");
															rc = INVALID_STATEMENT;
														}
													}

												}
												else {
													printf("1470");
													rc = INVALID_STATEMENT;
												}
											}
											else {
												printf("1475\n");
												rc = INVALID_STATEMENT;
											}
										}
									}
									else {
										where_exist = false;
										//printf("It shouldn't come here...");
									}

									//printf("Return code is: %d\n", rc);
									//printf("%d\n", where_exist);

									if (cur->next->tok_value == K_ORDER) {
										cur = cur->next;
										if (cur->next->tok_value == K_BY) {
											cur = cur->next;
											if (cur->next->tok_value == IDENT) {
												order_by_flag = true;
												orderbycolumn = cur->next->tok_string;
												cur = cur->next;
												if (cur->next->tok_value == K_DESC) {
													desc_flag = true;
												}
											}
											else {
												rc = INVALID_STATEMENT;
											}
										}
										else {
											rc = INVALID_STATEMENT;
										}
									}
									else {
										rc = INVALID_STATEMENT;
									}

									strcpy(filename, tab_entry->table_name);
									strcat(filename, ".tab");
									if ((fhandle = fopen(filename, "rbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else if (!rc)
									{
										printf("Start formating the result statements\n");
										//check to see if columns exist in the table
										_fstat(_fileno(fhandle), &file_stat);
										file_header = (table_file_header*)calloc(1, file_stat.st_size);
										fread(file_header, file_stat.st_size, 1, fhandle);
										fclose(fhandle);


										//read other rows here
										int i;

										//Allow for selecting all rows, hopefully
										if (star_flag) {
											for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++) {
												//sprintf(column_string + extra_colemn_len, "|%-16s", col_entry->col_name);
												//extra_colemn_len += sizeof("+----------------") - 1; // -1 because of null terminator

												char str1[sizeof(col_entry->col_name)];
												for (int t = 0; t < sizeof(col_entry->col_name); t++) {
													str1[t] = tolower(col_entry->col_name[t]);
												}
												strcpy(temp_col_entry[cur_id].col_name, str1);
												temp_col_entry[cur_id].col_id = cur_id;

												cur_id++;
											}
										}

										int column_size = cur_id;
										int sum_result[MAX_NUM_COL];
										int row_match_result[MAX_NUM_COL];

										//for (i = 0; i < tab_entry->num_columns; i++) //less than # of selected columns
										//printf("%d\n", cur_id);
										int extra_len = 0;
										for (i = 0; i < cur_id; i++)
										{
											//strcat(extra, "+----------------");
											//extra_len += snprintf(extra+extra_len, sizeof(extra)-extra_len, "+----------------", col_entry->col_name); //disuse snprintf in case of compiler problem
											//printf("%d\n%s\n", extra_len, extra);
											sprintf(extra + extra_len, "+----------------", extra);
											extra_len += sizeof("+----------------") - 1; // -1 because of null terminator
																						  //printf("%d\n", extra_len);
										}
										//strcat(extra, "+\n");
										//extra_len += snprintf(extra+extra_len, sizeof(extra)-extra_len, "+\n", col_entry->col_name); //disuse snprintf in case of compiler problem
										//printf("%d\n%s\n", extra_len, extra); 
										sprintf(extra + extra_len, "+\n", col_entry->col_name);
										extra_len += sizeof("+\n") - 1; // -1 because of null terminator
																		//printf("%d\n", extra_len);

																		//printf("%s\n", extra);
																		//read through the columns

										column_match = 0;
										//for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++){
										int extra_colemn_len = 0;
										//printf("cur_id is %d\n", cur_id);
										for (int n = 0; n < cur_id; n++) {
											//printf("|%-16s", col_entry->col_name);
											//add the columns to some list to check with entered columns
											//extra_len += snprintf(extra + extra_len, sizeof(extra) - extra_len, "|%-16s", col_entry->col_name); //disuse snprintf in case of compiler problem
											//for (int n = 0; n < cur_id; n++) {
											for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++) {
												char str1[sizeof(col_entry->col_name)]; //trying to store one of the string to a variable for comparison 
												for (int t = 0; t < sizeof(col_entry->col_name); t++) {
													str1[t] = tolower(col_entry->col_name[t]);
												}
												char str2[sizeof(temp_col_entry->col_name)]; //trying to store one of the string to a variable for comparison 
												for (int t = 0; t < sizeof(temp_col_entry[n].col_name); t++) {
													str2[t] = tolower(temp_col_entry[n].col_name[t]);
												}
												if (!stricmp(str1, str2)) { //If They are the same assuming case insensitive, concatenate the column name into it
													sprintf(column_string + extra_colemn_len, "|%-16s", col_entry->col_name);
													extra_colemn_len += sizeof("+----------------") - 1; // -1 because of null terminator
													column_match++;
												}
											}
										}

										///printf("%s|\n", extra);
										sprintf(column_string + extra_colemn_len, "|\n", col_entry->col_name);
										extra_colemn_len += sizeof("|\n") - 1;
										printf("%s%s%s", extra, column_string, extra);

										offset = 0;
										//for (int n = 0; n < cur_id; n++)
										for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < file_header->num_records; i++, selectedRow += file_header->record_size)
										{
											//printf("1254\n");
											for (int n = 0; n < cur_id; n++)
											{
												int t;

												if (where_cons != NULL) {
													//printf("1260\n");
													con_check = check_condition(tab_entry, selectedRow, where_cons);
												}

												//printf("The conditions are: %d %d\n", con_check, !where_exist);

												if (con_check || !where_exist) {
													for (t = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); t < tab_entry->num_columns; t++, col_entry++)
													{
														char str1[sizeof(col_entry->col_name)]; //trying to store one of the string to a variable for comparison
														int str1_len = sizeof(col_entry->col_name);
														int e;
														//for (e = 0; e < sizeof(col_entry->col_name); e++) {
														for (e = 0; e < str1_len; e++) {
															if (isalpha(col_entry->col_name[e])) {
																str1[e] = tolower(col_entry->col_name[e]);
															}
															else {
																str1[e] = (col_entry->col_name[e]);
															}
														}
														//printf("1194 ");
														char str2[sizeof(temp_col_entry->col_name)]; //trying to store one of the string to a variable for comparison 
														int str2_len = sizeof(temp_col_entry[n].col_name);
														//printf("1197 ");
														for (e = 0; e < str2_len; e++) {
															if (isalpha(col_entry->col_name[e])) {
																str2[e] = tolower(temp_col_entry[n].col_name[e]);
															}
															else {
																str2[e] = (col_entry->col_name[e]);
															}
														}
														//printf("1205");
														//if (strcmp(str1, str2) == 0)
														//{
														printf("%s %s\n", str1, str2);
														memcpy(&temp_size, selectedRow + offset, 1);
														offset++;

														if (strcmp(str1, str2) == 0)
														{
															printf("Process columns %s %s\n", str1, str2);
															//NULL
															if (temp_size == 0)
															{
																//printf("|%-16s", "NULL");
															}
															//INT
															else if (col_entry->col_type == T_INT)
															{
																printf("Integer processing...\n");
																memset(&temp_int, '\0', col_entry->col_len);
																memcpy(&temp_int, selectedRow + offset, temp_size);
																sum_match += temp_int;
																sum_result[n] += temp_int;
																//printf("%d %d\n",temp_int, sum_match);
																row_match++;
																row_match_result[n] += 1;
																//printf("%d\n", row_match);
																//printf("|%-16i", temp_int);
															}
															//STRING
															else
															{
																memset(tempString, '\0', MAX_IDENT_LEN + 1);
																memcpy(tempString, selectedRow + offset, col_entry->col_len);
																//printf("|%-16s", tempString);
																row_match++;
															}
														}
														offset += col_entry->col_len;
														//printf("%d\n", offset);
													}
													//printf("|\n");
													//printf("1712\n");
													offset = 0;
												}
												//printf("1715\n");
												//if (con_check || !where_exist) printf("|\n");
											}
											//if (con_check || !where_exist) printf("|\n");
											//printf("1719\n");
										}// End of loop going through the rows
										//printf("1721\n");
										if (avg_flag && row_match != 0 && !star_flag) {
											printf("|%-16i", sum_match / row_match);
										}
										else if (sum_flag && !star_flag) {
											printf("|%-16i", sum_match);
										}
										else if (count_flag && !star_flag) {
											printf("|%-16i", row_match);
										}
										else if (avg_flag && row_match != 0 && star_flag) {
											//printf("1732\n");
											for (int n = 0; n < cur_id; n++) {
												if (row_match_result[n] != 0)
												{
													printf("|%-16i", sum_result[n] / row_match_result[n]);
												}
												else {
													char *ptr = "";
													printf("%16s\n", ptr);
													rc = INVALID_STATEMENT;
												}
											}
										}
										else if (sum_flag && star_flag) {
											for (int n = 0; n < cur_id; n++) {
												if (row_match_result[n] != 0)
												{
													printf("|%-16i", sum_result[n]);
												}
												else {
													rc = INVALID_STATEMENT;
												}
											}
										}
										else if (count_flag && star_flag) {
											for (int n = 0; n < cur_id; n++) {
												printf("|%-16i", row_match_result[n]);
											}
										}
										printf("|\n");
									}
									//printf("1335\n");
									printf("%s\n", extra);
									//printf("%d", column_match);
								}
								else {
									rc = INVALID_STATEMENT;
								}
							}

						}// From clause ends here
					}
				}
				else {
					rc = INVALID_STATEMENT;
				}
			}
		}
		else {
			rc = INVALID_STATEMENT;
		}
	}
	else {
		rc = INVALID_COLUMN_DEFINITION;
	}
	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables stored in the database\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End Of List ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN + 1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if ((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);
						}

						/* Next, write the cd_entry information */
						for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
						i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if ((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								(sizeof(tpd_list) - sizeof(tpd_entry)),
								1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
							1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								((char*)cur - (char*)g_tpd_list),
								1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}


			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int sem_delete(token_list *t_list) {
	token_list* cur;
	token_list* insert_back = (token_list*)malloc(sizeof(token_list));
	tpd_entry *tab_entry = (tpd_entry*)malloc(sizeof(tpd_entry));
	cd_entry  *col_entry = (cd_entry*)malloc(sizeof(cd_entry));
	table_file_header *new_file_header = NULL;
	table_file_header *file_header = (table_file_header*)malloc(sizeof(table_file_header));
	where_conds *where_cons = (where_conds*)malloc(sizeof(where_conds));
	FILE *fhandle = NULL;
	row * selectedRow = NULL;
	row * row_offset = NULL;
	int rc = 0;
	int temp_int = 0;
	int temp_size = 0;
	int offset = 0;
	char* tableName;
	char tempString[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	struct _stat file_stat;

	cur = t_list;
	//printf("1699\n");

	if (cur->tok_value == S_STAR) {
		cur = cur->next;
		if (cur->tok_value == K_FROM) {
			cur = cur->next;
			if (cur->tok_value == IDENT) {
				if (cur->next->tok_value == EOC) {
					if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
					{
						rc = TABLE_NOT_EXIST;
						cur->tok_value = INVALID_TABLE_NAME;
					}
					else
					{
						//read from file and print
						strcpy(filename, tab_entry->table_name);
						strcat(filename, ".tab");
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
						else {
							_fstat(_fileno(fhandle), &file_stat);
							file_header = (table_file_header*)calloc(1, file_stat.st_size);
							fread(file_header, file_stat.st_size, 1, fhandle);
							fclose(fhandle);

							printf("%d rows deleted.\n", file_header->num_records);

							file_header->num_records = 0;
							file_header->file_size = sizeof(table_file_header);

							fhandle = fopen(filename, "wb");
							fwrite(file_header, sizeof(table_file_header), 1, fhandle);
							fflush(fhandle);
							fclose(fhandle);
						}
					}
				}
				else {
					rc = INVALID_STATEMENT;
				}
			}
			else {
				rc = INVALID_STATEMENT;
			}
		}
		else {
			rc = INVALID_STATEMENT;
		}
	}
	else if (cur->tok_value == K_FROM) {
		//printf("1752\n");
		cur = cur->next;
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) != NULL) {
			//printf("%s\n", tab_entry->table_name);
			tableName = cur->tok_string;
			cur = cur->next;
			//printf("1756\n");
			if (cur->tok_value == EOC) {
				strcpy(filename, tab_entry->table_name);
				strcat(filename, ".tab");
				if ((fhandle = fopen(filename, "rbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else {
					_fstat(_fileno(fhandle), &file_stat);
					file_header = (table_file_header*)calloc(1, file_stat.st_size);
					fread(file_header, file_stat.st_size, 1, fhandle);
					fclose(fhandle);

					printf("%d rows deleted.\n", file_header->num_records);

					file_header->num_records = 0;
					file_header->file_size = sizeof(table_file_header);

					fhandle = fopen(filename, "wb");
					fwrite(file_header, sizeof(table_file_header), 1, fhandle);
					fflush(fhandle);
					fclose(fhandle);
				}
			}
			else if (cur->tok_value == K_WHERE) {
				//printf("%s\n", tab_entry->table_name);
				cur = cur->next;
				if (cur->tok_value == IDENT) {
					//printf("1783\n");
					if (cur->next->tok_value == S_EQUAL || cur->next->tok_value == S_LESS || cur->next->tok_value == S_GREATER) {
						//printf("1784\n");
						if ((cur->next->next->tok_value == STRING_LITERAL || cur->next->next->tok_value == INT_LITERAL)) { //&& cur->next->next->next->next->tok_value == EOC) {
							//printf("1786\n");
							//rintf("%s\n", tab_entry->table_name);
							if (cur->next->next->next->tok_value == EOC) {

								int del_row[MAX_ROWS] = {};
								int count = 0;
								int i = 0;

								where_cons->col_name = cur->tok_string;
								where_cons->op_tok_value = cur->next->tok_value;
								where_cons->val = cur->next->next->tok_string;
								where_cons->val_tok_value = cur->next->next->tok_value;
								where_cons->next = NULL;

								strcpy(filename, tab_entry->table_name);
								strcat(filename, ".tab");

								if ((fhandle = fopen(filename, "rbc")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
								}
								else
								{
									_fstat(_fileno(fhandle), &file_stat);
									file_header = (table_file_header*)calloc(1, file_stat.st_size);
									fread(file_header, file_stat.st_size, 1, fhandle);
									fclose(fhandle);


									bool con_check;
									for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < file_header->num_records; i++, selectedRow += file_header->record_size)
									{
										printf("Going through rows to check whether any of them meet the conditions\n");
										con_check = check_condition(tab_entry, selectedRow, where_cons);
										printf("The condition is: %d\n", con_check);
										if (con_check)
										{
											//set it to 1
											del_row[i]++;
											count++;
										}
									}

									int new_size = file_header->file_size - count*file_header->record_size;
									new_file_header = (table_file_header*)calloc(1, new_size);
									memcpy(new_file_header, file_header, sizeof(table_file_header));
									offset = file_header->record_offset;
									for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset);
									i < file_header->num_records; i++, selectedRow += file_header->record_size)
									{
										if (del_row[i] == 0)
										{
											//printf("%s\n", selectedRow);
											memcpy((void*)((char*)new_file_header + offset), selectedRow, file_header->record_size);
											offset += file_header->record_size;
										}

									}

									new_file_header->num_records = file_header->num_records - count;
									new_file_header->file_size = new_size;

									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_file_header, new_file_header->file_size, 1, fhandle);
										fflush(fhandle);
										fclose(fhandle);
										file_header = new_file_header;

										printf("%d rows deleted.\n", count);
									}
								}
								//previous code for doing delete. Now here for reference
								/*
								where_cons->col_name = cur->tok_string;
								where_cons->op_tok_value = cur->next->tok_value;
								where_cons->val = cur->next->next->tok_string;
								where_cons->val_tok_value = cur->next->next->tok_value;
								where_cons->next = NULL;

								strcpy(filename, tab_entry->table_name);
								strcat(filename, ".tab");

								if ((fhandle = fopen(filename, "rbc")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
								}
								else
								{
									_fstat(_fileno(fhandle), &file_stat);
									file_header = (table_file_header*)calloc(1, file_stat.st_size);
									fread(file_header, file_stat.st_size, 1, fhandle);
									fclose(fhandle);

									int orig_num_records = file_header->num_records;

									file_header->num_records = 0;
									file_header->file_size = sizeof(table_file_header);

									fhandle = fopen(filename, "wb");
									fwrite(file_header, sizeof(table_file_header), 1, fhandle);
									fflush(fhandle);
									fclose(fhandle);


									//read other rows here
									int i;
									int j;
									int deleted_rows = 0;
									int undeleted_rows = 0;

									//printf("1835\n");
									//for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < file_header->num_records; i++, selectedRow += file_header->record_size)
									for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < orig_num_records; i++, selectedRow += file_header->record_size)
									{
										offset = 0;
										bool del_flag = false;

										//printf("%d", tab_entry->num_columns);

										for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); j < tab_entry->num_columns; j++, col_entry++)
										{
											//printf("1839\n");
											memcpy(&temp_size, selectedRow + offset, 1);
											offset++;

											memset(tempString, '\0', MAX_IDENT_LEN + 1);
											memcpy(tempString, selectedRow + offset, col_entry->col_len);
											//printf("%s", tempString);

											//printf("%s, %s\n", col_entry->col_name, where_cons->col_name);
											if (strcmp(col_entry->col_name, where_cons->col_name) == 0) {
												//printf("1835\n");
												char* str1;
												if (temp_size == 0)
												{
													str1 = "NULL";
													//printf("1840\n");
												}
												else if (col_entry->col_type == INT_LITERAL)
												{
													//printf("1838 ");
													memset((&temp_int), ('\0'), (col_entry->col_len));
													memcpy((&temp_int), (selectedRow + offset), (temp_size));
													sprintf(str1, "%d", temp_int);
													//printf("1848\n");
												}
												else
												{
													memset(tempString, '\0', MAX_IDENT_LEN + 1);
													memcpy(tempString, selectedRow + offset, col_entry->col_len);
													str1 = tempString;
													//printf("1855\n");
												}
												if (strcmp(str1, where_cons->val) == 0) {
													//printf("1867\n");
													deleted_rows++;
													del_flag = true;
												}
											}
											offset += col_entry->col_len;
										}
										if (!del_flag) {
											undeleted_rows++;
											int newSize = file_header->file_size + (file_header->record_size)*undeleted_rows;
											//printf("This is it here: %d, %d\n", newSize, undeleted_rows);
											table_file_header* new_file_header = (table_file_header*)calloc(1, newSize);
											memcpy(new_file_header, file_header, file_header->file_size);
											memcpy((void*)((char*)new_file_header + file_header->file_size), (void*)selectedRow, file_header->record_size);

											//printf("%s\n", selectedRow);

											new_file_header->file_size = newSize;
											new_file_header->num_records++;

											if ((fhandle = fopen(filename, "wbc")) == NULL)
											{
												rc = FILE_OPEN_ERROR;
											}
											else
											{
												fwrite(new_file_header, new_file_header->file_size, 1, fhandle);
												fflush(fhandle);
												fclose(fhandle);
												file_header = new_file_header;
											}

											if ((fhandle = fopen(filename, "wbc")) == NULL)
											{
												rc = FILE_OPEN_ERROR;
											}
											else
											{
												fwrite(new_file_header, new_file_header->file_size, 1, fhandle);
												fflush(fhandle);
												fclose(fhandle);
												file_header = new_file_header;
											}
											del_flag = false;
										}
										else {
											//printf("1902\n");
										}
									}
									printf("%d rows deleted.", deleted_rows);
								}
								*/
							}
							else {
								rc = INVALID_STATEMENT;
							}
						}
						else {
							rc = INVALID_STATEMENT;
						}
					}
					else {
						rc = INVALID_STATEMENT;
					}
				}
				else {
					rc = INVALID_STATEMENT;
				}
			}
			else {
				rc = INVALID_STATEMENT;
			}
		}
		else {
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID_TABLE_NAME;
		}
	}
	else {
		rc = INVALID_STATEMENT;
	}
	return rc;
}

int sem_update(token_list *t_list) {
	token_list* cur;
	tpd_entry *tab_entry;
	cd_entry  *col_entry;
	set_conds *set_cons = (set_conds*)malloc(sizeof(set_conds));
	where_conds *where_cons = NULL;
	table_file_header *file_header = NULL;
	FILE *fhandle = NULL;
	row* selectedRow;
	bool con_check = true;
	bool no_where = true;
	int rc = 0;
	int offset = 0;
	int temp_size = 0;
	int temp_int = 0;
	int update_rows = 0;
	int temp_cmp_int = 0;
	char* tableName;
	char temp_cmp_str[MAX_IDENT_LEN + 1] = {};
	char filename[MAX_IDENT_LEN + 1];
	char tempString[MAX_IDENT_LEN + 1];
	struct _stat file_stat;

	cur = t_list;

	//if (cur->tok_value == K_FROM) {
		//printf("We are in the update statement\n");
		//cur = cur->next;
	if ((tab_entry = get_tpd_from_list(cur->tok_string)) != NULL) {
		//printf("Table does exist\n");
		tableName = cur->tok_string;
		cur = cur->next;
		if (cur->tok_value == K_SET) {
			cur = cur->next;
			token_list* temp_cur = cur;
			//printf("%s, %d\n", temp_cur->tok_string, temp_cur->tok_value);
			//printf("Parsing SET statements\n");
			if (temp_cur->tok_value == IDENT) {
				set_cons->col_name = temp_cur->tok_string;
				if (temp_cur->next->tok_value == S_EQUAL) {
					//printf("2067\n");
					if ((temp_cur->next->next->tok_value == INT_LITERAL || temp_cur->next->next->tok_value == STRING_LITERAL)) {
						//printf("2154\n");
						set_cons->val = temp_cur->tok_string;
						set_cons->val_tok_value = temp_cur->tok_value;
						if (temp_cur->next->next->next->tok_value != EOC && temp_cur->next->next->next->tok_value == K_WHERE) {// Assume we have a where clause
							//printf("Where Clause");
							temp_cur = temp_cur->next->next->next;
							//cur = temp_cur;
							where_cons = (where_conds*)malloc(sizeof(where_conds));
							//printf("2074\n");
							no_where = false;
							if (temp_cur->next->tok_value == IDENT) {
								//printf("2076\n");
								if (temp_cur->next->next->tok_value == S_EQUAL || temp_cur->next->next->tok_value == S_LESS || temp_cur->next->next->tok_value == S_GREATER) {
									if ((temp_cur->next->next->next->tok_value == STRING_LITERAL || temp_cur->next->next->next->tok_value == INT_LITERAL)) {
										//printf("2079\n");
										if (temp_cur->next->next->next->next->tok_value == EOC) {
											//printf("Where Clause...\n");
											where_cons->col_name = temp_cur->next->tok_string;
											//printf("WHERE Coleumn name is: %s\n", where_cons->col_name);
											where_cons->op_tok_value = temp_cur->next->next->tok_value;
											//printf("%d\n", where_cons->op_tok_value);
											where_cons->val = temp_cur->next->next->next->tok_string;
											//printf("%s\n", where_cons->val);
											where_cons->val_tok_value = temp_cur->next->next->next->tok_value;
											//printf("%d\n", where_cons->val_tok_value);
											where_cons->next = NULL;
										}
										else {
											rc = INVALID_STATEMENT;
										}
									}
									else {
										rc = INVALID_STATEMENT;
									}
								}
								else {
									rc = INVALID_STATEMENT;
								}
							}
							else {
								rc = INVALID_STATEMENT;
							}
						}
						//printf("1902\n");
						(set_cons->col_name) = cur->tok_string;
						//printf("%s\n", set_cons->col_name);
						//printf("2071\n");
						(set_cons->val) = cur->next->next->tok_string;
						//printf("%s\n", set_cons->val);
						//printf("2073\n");
						set_cons->val_tok_value = (cur->next->next->tok_value);
						//printf("%d\n", set_cons->val_tok_value);
						//temp_cur = temp_cur->next->next->next;
						//printf("Here: %d\n", temp_cur->tok_value); // May get null value.
						strcpy(filename, tab_entry->table_name);
						strcat(filename, ".tab");

						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
						else {

							_fstat(_fileno(fhandle), &file_stat);
							file_header = (table_file_header*)calloc(1, file_stat.st_size);
							fread(file_header, file_stat.st_size, 1, fhandle);
							fclose(fhandle);

							//printf("1911\n");
							//read other rows here
							int i;
							int j;

							//printf("Set information is: %s %s %d\n", set_cons->col_name, set_cons->val, set_cons->val_tok_value);

							//printf("Num_records: %d\n", file_header->num_records);

							for (i = 0, selectedRow = (row*)((char*)file_header + file_header->record_offset); i < file_header->num_records; i++, selectedRow += file_header->record_size)
							{

								//	printf("%d\n", i);
								offset = 0;

								if (where_cons != NULL) {
									//printf("Checking the conditions... Column is this: %s\n", where_cons->col_name);
									con_check = check_condition(tab_entry, selectedRow, where_cons);
									//printf("Result of check condition is: %d\n", con_check);
								}
								else {
									con_check = true;
								}

								if (con_check)
								{
									//printf("Entered the coleumn loop...\n");
									for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); j < tab_entry->num_columns; j++, col_entry++)
									{

										//printf("%s\n", col_entry->col_name);
										//printf("Line 2255 comparing... %s, %s.\n", set_cons->col_name, col_entry->col_name);

										memcpy(&temp_size, selectedRow + offset, 1);
										offset++;


										if (!strcmp(set_cons->col_name, col_entry->col_name))
										{

											//printf("%s\n", set_cons->col_name);
											//printf("Checking what type the value is...\n");
											if (!strcmp(set_cons->val, "NULL"))
											{
												return false; //base on SQL syntax, if two values are NULL, they are considered different
											}

											else if (col_entry->col_type == T_INT)
											{
												//offset++;

												if (set_cons->val_tok_value == INT_LITERAL) {
													//printf("It's an integer\n");
													temp_cmp_int = 0;
													memcpy(&temp_cmp_int, selectedRow + offset, sizeof(int));
													int x = atoi(set_cons->val);
													memcpy(selectedRow + offset, &x, 4);
													//printf("%d\n", temp_cmp_int);
													update_rows++;
												}
											}

											//string
											else
											{
												//printf("It's an String\n");
												if (set_cons->val_tok_value == STRING_LITERAL) {
													memset(temp_cmp_str, '\0', MAX_IDENT_LEN + 1);
													memcpy(temp_cmp_str, selectedRow + offset, temp_size);
													//memcpy(selectedRow + offset, set_cons->val, sizeof(set_cons->val));
													memcpy(selectedRow + offset, set_cons->val, temp_size);
													//printf("String is %s, size is %d\n", temp_cmp_str, sizeof(set_cons->val));
													//printf("String is %s, size is %d\n", temp_cmp_str, sizeof(set_cons->val));
													update_rows++;
												}
											}
										}

										offset += col_entry->col_len;
									}
								}

								//printf("|\n");
								//printf("1956\n");

							//}
							//printf("i: %d Num_records: %d\n", i, file_header->num_records);
							}
							//printf("%d", rc);
						}
						if (!rc)
						{
							//printf("2232\n");
							if ((fhandle = fopen(filename, "wbc")) == NULL)
							{
								rc = FILE_OPEN_ERROR;
							}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
							else
							{
								fwrite(file_header, file_header->file_size, 1, fhandle);
								printf("%d rows updated\n", update_rows);
							}
						}
					}// Assume that it's a literal here
					else {
						printf("2234\n");
						rc = INVALID_STATEMENT;
					}
				}
				else {
					printf("2239\n");
					rc = INVALID_STATEMENT;
				}
			}
			else {
				printf("2244\n");
				rc = INVALID_STATEMENT;
			}
			//printf("1982");
		}
		else {
			printf("2257\n");
			rc = INVALID_STATEMENT;
		}
		//printf("1986");
	}
	else {
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID_TABLE_NAME;
	}
	//}
	//printf("1991");
	return rc;
}

bool check_condition(tpd_entry *tab_entry, row * selectedRow, where_conds *where_con)
{
	char temp_cmp_str[MAX_IDENT_LEN + 1] = {};
	int temp_cmp_int;
	cd_entry *col_entry = NULL;
	int j;
	int offset = 0;
	int temp_size = 0;
	bool result = false;
	char* tempString;
	int* temp_int;

	row* origRow = selectedRow;

	//printf("In the check condition stage\n");
	//printf("%d\n", tab_entry->num_columns);

	for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); j < tab_entry->num_columns; j++, col_entry++)
	{
		//printf("Offset is  %d\n", offset);
		//printf("%d loops\n", j);
		//printf("Going through the columns\n");
		//printf("%s, %s.\n", where_con->col_name, col_entry->col_name);

		//offset += (1 + col_entry->col_len);
		memcpy(&temp_size, selectedRow + offset, 1);
		//printf("%d", temp_size);

		memset(temp_cmp_str, '\0', col_entry->col_len);
		memcpy(temp_cmp_str, selectedRow + offset + 1, temp_size);

		//printf("String in memory is: %s\n", temp_cmp_str);

		//printf("%s %s %s\n", where_con->col_name, where_con->val, col_entry);

		//offset += (1 + col_entry->col_len);

		//printf("Results of comparing the column name is: %d\n", strcmp(where_con->col_name, col_entry->col_name));

		offset++;

		if (!strcmp(where_con->col_name, col_entry->col_name))
		{
			//printf("2176\n");
			//printf("Checking what type the value is...\n");
			if (!strcmp(where_con->val, "NULL"))
			{
				return false; //base on SQL syntax, if two values are NULL, they are considered different
			}
			else if (col_entry->col_type == T_INT)
			{
				//offset++;
				if (where_con->val_tok_value == INT_LITERAL) {

					//printf("Number comparison\n");
					temp_cmp_int = 0;
					memcpy(&temp_cmp_int, selectedRow + offset, sizeof(int));
					//printf("%d %d\n", temp_cmp_int, atoi(where_con->val));
					//printf("%s\n", (selectedRow + offset + 1));

					//printf("Integer from memory is: %d\n", temp_cmp_int);

					if (where_con->op_tok_value == S_LESS && temp_cmp_int < atoi(where_con->val))
						result = true;
					else if (where_con->op_tok_value == S_GREATER && temp_cmp_int > atoi(where_con->val))
						result = true;
					//must be equal
					else if (where_con->op_tok_value == S_EQUAL && temp_cmp_int == atoi(where_con->val))
						result = true;
				}
				//printf("Result: %d\n", result);
			}
			//string
			else
			{
				//offset++;
				if (where_con->val_tok_value == STRING_LITERAL) {
					//printf("String comparison...\n");
					//memset(temp_cmp_str, '\0', col_entry->col_len);
					memset(temp_cmp_str, '\0', MAX_IDENT_LEN + 1);
					memcpy(temp_cmp_str, selectedRow + offset, temp_size);
					//printf("String from memory is: %s, and the string in WHERE condition is: %s\n", temp_cmp_str, where_con->val);
					if (where_con->op_tok_value == S_LESS && (strcmp(temp_cmp_str, where_con->val) < 0))
						result = true;
					else if (where_con->op_tok_value == S_GREATER && (strcmp(temp_cmp_str, where_con->val) > 0))
						result = true;
					//must be equal
					else if (where_con->op_tok_value == S_EQUAL && (strcmp(temp_cmp_str, where_con->val) == 0))
						result = true;
					//printf("Result: %d\n", result);
				}
			}
			//printf("2401\n");
		}
		//printf("2402\n");
		offset += col_entry->col_len;
		//printf("Coluemn length is: %d\n", col_entry->col_len);
		//offset++;
		//printf("%d\n", offset);
		//printf("2405\n");

	}
	//printf("2407\n");
	if (where_con->next == NULL) {
		//printf("Result is %d\n",result);
		return result;
	}
	else {
		//printf("2414\n");
		if (where_con->next->op_tok_value == K_AND)
		{
			return result && (check_condition(tab_entry, origRow, where_con->next));
		}
		else
		{
			return result || (check_condition(tab_entry, origRow, where_con->next));
		}
	}
}

//The code for backing up the current tables and table definitions
int sem_backup(token_list *t_list) {
	token_list* cur;
	char* filename;
	char* temp_filename;
	char* data = NULL;
	FILE* file;
	FILE* temp;
	int rc = 0;
	int num_tables = 0;
	table_file_header* file_header = NULL;

	cur = t_list;

	if (cur->next->tok_value == IDENT) {
		cur = cur->next;
		filename = (char*)calloc(strlen(cur->tok_string) + 3, sizeof(char));
		strcat(filename, cur->tok_string);
		strcat(filename, ".bk");
		if (cur->next->tok_value == EOC) {
			struct stat buffer;
			tpd_entry * cur_tab = &(g_tpd_list->tpd_start);

			if (!(stat(filename, &buffer) == 0)) {
				struct _stat file_stat;
				file = fopen(filename, "wb");
				temp = fopen("dbfile.bin", "rb");
				_fstat(_fileno(temp), &file_stat);
				fwrite(g_tpd_list, g_tpd_list->list_size, 1, file);
				fflush(temp);
				fclose(temp);

				if (g_tpd_list->list_size == file_stat.st_size)
				{
					while (num_tables-- > 0)
					{
						temp_filename = (char *)calloc(strlen(cur_tab->table_name) + 5, sizeof(char));
						strcat(temp_filename, cur_tab->table_name);
						strcat(temp_filename, ".dat");
						temp = fopen(temp_filename, "rb");
						_fstat(_fileno(temp), &file_stat);
						fread(file_header, sizeof(table_file_header), 1, temp);
						fseek(temp, 0, SEEK_SET);
						if (file_header->file_size == file_stat.st_size)
						{
							int size = file_stat.st_size;
							fwrite(&size, sizeof(int), 1, file);
							data = (char*)calloc(size, sizeof(char));
							fread(data, size, 1, temp);
							fwrite(data, size, 1, file);
							if (num_tables > 0)
							{
								cur_tab = (tpd_entry*)((char*)cur_tab + cur_tab->tpd_size);
							}
						}
						else
						{
							rc = DBFILE_CORRUPTION;
							printf("Table File: %s is corrupted\n", cur_tab->table_name);
							printf("Real Size:%d != Actual Size:%d\n\n", file_header->file_size, file_stat.st_size);
							fflush(file);
							fclose(file);
							fflush(temp);
							fclose(temp);
							remove(filename);
							return rc;
						}
					}
				}
				else
				{
					rc = DBFILE_CORRUPTION;
					printf("DbFile is corrupted\n");
					printf("Real Size:%d != Actual Size:%d\n\n", g_tpd_list->list_size, file_stat.st_size);
					fflush(file);
					fclose(file);
					fflush(temp);
					fclose(temp);
				}

				fflush(file);
				fclose(file);
				fflush(temp);
				fclose(temp);
			}
			else {
				rc = BACKUP_FILE_ALREADY_EXIST;
				printf("A backup file with the same name already existed.\n");
			}
		}
	}
	else {
		rc = INVALID_STATEMENT;
	}

	return rc;
}

int sem_restore(token_list *t_list)
{
	int rc = 0;
	token_list *cur = NULL;
	cur = t_list;
	if (cur->tok_value == IDENT)
	{
		FILE *fp = NULL;
		FILE *fp2 = NULL;
		char *buffer = NULL;
		char *temp_char = NULL;
		char *temp = NULL;
		char * b_file_name = (char *)calloc(strlen(cur->tok_string) + 4, sizeof(char));
		strcat(b_file_name, cur->tok_string);
		strcat(b_file_name, ".bk");
		char * l_file_name = (char *)calloc(7, sizeof(char));
		strcat(l_file_name, "db.old");
		char *t_file_name = NULL;
		rename("db.log", "db.old");
		temp = (char *)calloc(strlen(cur->tok_string) + 11, sizeof(char));
		strcat(temp, "BACKUP ");
		strcat(temp, cur->tok_string);
		temp[strlen(cur->tok_string) + 11] = '\0';
		buffer = (char *)calloc(256, sizeof(char));
		temp_char = (char *)calloc(256, sizeof(char));
		int size = NULL;

		struct stat buffer2;
		tpd_entry * cur_tab = &(g_tpd_list->tpd_start);

		if (stat(b_file_name, &buffer2) == 0)
		{
			if (stat(l_file_name, &buffer2) == 0)
			{
				if ((cur->next->tok_value == EOC)
					|| ((cur->next->tok_value == K_WITHOUT) && (cur->next->next->tok_value == K_RF)))
				{
					if (cur->next->tok_value == EOC)
					{
						fp = fopen(l_file_name, "r+");
						fp2 = fopen("db.log", "w");
						while (fgets(buffer, 256, fp) != NULL)
						{
							memcpy(temp_char, buffer, 256);
							buffer[6] = '\0';
							if (strcmp(buffer, "BACKUP\0") != 0)
							{
								fprintf(fp2, "%s", temp_char);
							}
							else if (strcmp(buffer, "BACKUP\0") == 0)
							{
								temp_char[strlen(temp_char) - 1] = '\0';
								if (strcmp(temp_char, temp) == 0)
								{
									fprintf(fp2, "%s\n", temp_char);
									fprintf(fp2, "%s\n", "RF_START");
									FILE *in = NULL;
									FILE *tout = NULL;
									tpd_list *tlist = NULL;
									in = fopen(b_file_name, "rb");
									unsigned char *file_buffer = NULL;
									struct _stat file_stat;
									_fstat(_fileno(in), &file_stat);
									file_buffer = (unsigned char*)calloc(file_stat.st_size, sizeof(char)); //Buffer long enough
									tout = fopen("dbfile.bin", "wb");
									fread(&size, sizeof(int), 1, in);
									fseek(in, 0, SEEK_SET);
									fread(file_buffer, size, 1, in);
									fseek(in, 0, SEEK_SET);
									tlist = (tpd_list*)calloc(1, size);
									fread(tlist, size, 1, in);
									tlist->db_flags = ROLLFORWARD_PENDING;
									fwrite(tlist, size, 1, tout);
									tpd_entry *tab = &(tlist->tpd_start);
									int num_tables = tlist->num_tables;
									fflush(tout);
									fclose(tout);
									while (num_tables-- > 0)
									{
										t_file_name = (char *)calloc(strlen(tab->table_name) + 6, sizeof(char));
										strcat(t_file_name, tab->table_name);
										strcat(t_file_name, ".dat");;
										tout = fopen(t_file_name, "wb");
										fread(&size, sizeof(int), 1, in);
										fread(file_buffer, size, 1, in);
										fwrite(file_buffer, size, 1, tout);
										if (num_tables > 0)
										{
											tab = (tpd_entry*)((char*)tab + tab->tpd_size);
										}
										fflush(tout);
										fclose(tout);
										free(t_file_name);
									}
								}
								else
								{
									fprintf(fp2, "%s\n", temp_char);
								}
							}
							else
							{
								rc = BACKUP_FILE_ALREADY_EXIST;
								printf("ERROR: MISSING LOG BAKCUP IN THE LOG  FILE\n");
							}
						}
						fflush(fp);
						fclose(fp);
						fflush(fp2);
						fclose(fp2);
						free(buffer);
						free(temp_char);
						remove("db.old");
					}
					else if (cur->next->tok_value == K_WITHOUT)
					{
						fp = fopen(l_file_name, "r+");
						fp2 = fopen("db.log", "w");
						while (fgets(buffer, 256, fp) != NULL)
						{
							memcpy(temp_char, buffer, 256);
							buffer[6] = '\0';
							if (strcmp(buffer, "BACKUP\0") != 0)
							{
								fprintf(fp2, "%s", temp_char);
							}
							else if (strcmp(buffer, "BACKUP\0") == 0)
							{
								temp_char[strlen(temp_char) - 1] = '\0';
								if (strcmp(temp_char, temp) == 0)
								{
									fprintf(fp2, "%s\n", temp_char);
									FILE *in = NULL;
									FILE *tout = NULL;
									tpd_list *tlist = NULL;
									in = fopen(b_file_name, "rb");
									unsigned char *file_buffer = NULL;
									struct _stat file_stat;
									_fstat(_fileno(in), &file_stat);
									file_buffer = (unsigned char*)calloc(file_stat.st_size, sizeof(char)); //Buffer long enough
									tout = fopen("dbfile.bin", "wb");
									fread(&size, sizeof(int), 1, in);
									fseek(in, 0, SEEK_SET);
									fread(file_buffer, size, 1, in);
									fseek(in, 0, SEEK_SET);
									tlist = (tpd_list*)calloc(1, size);
									fread(tlist, size, 1, in);
									fwrite(file_buffer, size, 1, tout);
									tpd_entry *tab = &(tlist->tpd_start);
									int num_tables = tlist->num_tables;
									fflush(tout);
									fclose(tout);
									while (num_tables-- > 0)
									{
										t_file_name = (char *)calloc(strlen(tab->table_name) + 6, sizeof(char));
										strcat(t_file_name, tab->table_name);
										strcat(t_file_name, ".dat");;
										tout = fopen(t_file_name, "wb");
										fread(&size, sizeof(int), 1, in);
										fread(file_buffer, size, 1, in);
										fwrite(file_buffer, size, 1, tout);
										if (num_tables > 0)
										{
											tab = (tpd_entry*)((char*)tab + tab->tpd_size);
										}
										fflush(tout);
										fclose(tout);
										free(t_file_name);
									}
									break;
								}
								else
								{
									fprintf(fp2, "%s\n", temp_char);
								}
							}
							else
							{
								rc = BACKUP_LOG_MISSING;
								printf("The backup log file is missing\n");
							}
						}
						if (fgets(buffer, 256, fp) != NULL)
						{
							int k = 1;
							char number[5];
							char *file_name = (char *)calloc(10, sizeof(char));
							strcat(file_name, "db.log");
							//			    			itoa(k, number, 10);
							sprintf(number, "%d", k);
							strcat(file_name, number);
							while ((stat(file_name, &buffer2) == 0))
							{
								k++;
								free(file_name);
								file_name = (char *)calloc(10, sizeof(char));
								strcat(file_name, "db.log");
								//			    				itoa(k, number, 10);
								sprintf(number, "%d", k);
								strcat(file_name, number);
							}
							fflush(fp);
							fclose(fp);
							rename("db.old", file_name);

						}
						fflush(fp);
						fclose(fp);
						fflush(fp2);
						fclose(fp2);
						free(buffer);
						free(temp_char);
					}
					else
					{
						rename("db.old", "db.log");
						rc = LOG_MISSING;
						cur->tok_value = INVALID;
						printf("Log file does not exist at this time.");
					}
				}
				else
				{
					rename("db.old", "db.log");
					rc = INVALID_STATEMENT;
					cur->next->tok_value = INVALID;
				}
			}
		}
		else
		{
			rename("db.old", "db.log");
			rc = BACKUP_MISSING;
			cur->tok_value = INVALID;
			printf("Backup file does not exist at this time.");
		}
	}
	else
	{
		rc = INVALID_VALUE;
		cur->tok_value = INVALID;
	}
	return rc;
}

int sem_rollforward(token_list *t_list)
{
	int rc = 0;
	token_list *cur = NULL;
	cur = t_list;
	FILE *fp;
	FILE *fp2;
	FILE *dbfile;
	FILE *aux;
	char *buffer;
	char *temp_char;
	char *tolog;
	char *date;
	bool found = false;
	time_t rtime1;
	time_t rtime2;
	bool prune = false;
	if (cur->tok_value == EOC)
	{
		struct stat buffer2;
		tpd_entry * cur_tab = &(g_tpd_list->tpd_start);

		if ((stat("db.log", &buffer2) == 0))
		{
			fp = fopen("dbfile.bin", "wb");
			g_tpd_list->db_flags = 0;
			fwrite(g_tpd_list, g_tpd_list->list_size, 1, fp);
			fflush(fp);
			fclose(fp);
			fp = NULL;
			rename("db.log", "db.old");
			fp = fopen("db.old", "r");
			aux = fopen("db.log", "w");
			buffer = (char *)calloc(500, sizeof(char));
			temp_char = (char *)calloc(20, sizeof(char));
			while (fgets(buffer, 500, fp) != NULL)
			{
				token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;
				if (found == true)
				{
					fprintf(aux, "%s", buffer);
					buffer[strlen(buffer) - 2] = '\0';
					get_token((buffer + 16), &tok_list);
					do_semantic(tok_list);
				}
				if (found == false)
				{
					memcpy(temp_char, buffer, 20);
					temp_char[8] = '\0';
					if (strcmp(temp_char, "RF_START\0") == 0)
					{
						found = true;
						g_tpd_list->db_flags = ROLLING_FORWARD;
						fgets(buffer, 500, fp);
						fprintf(aux, "%s", buffer);
						buffer[strlen(buffer) - 2] = '\0';
						get_token((buffer + 16), &tok_list);
						do_semantic(tok_list);
						tok_ptr = tok_list;
					}
					else
					{
						fprintf(aux, "%s", buffer);
					}
					tok_ptr = tok_list;
					while (tok_ptr != NULL)
					{
						tmp_tok_ptr = tok_ptr->next;
						free(tok_ptr);
						tok_ptr = tmp_tok_ptr;
					}
				}

			}
			if (found == false)
			{
				rc = RF_START_MISSING;
				cur->tok_value = INVALID;
			}
			fflush(fp);
			fclose(fp);
			fp = NULL;
			fflush(aux);
			fclose(aux);
			aux = NULL;
		}
		else
		{
			rc = LOG_MISSING;
			cur->tok_value = INVALID;
		}
		free(buffer);
		free(temp_char);
	}
	else if (cur->tok_value == K_TO)
	{
		if (cur->next->tok_value != EOC)
		{
			if (cur->next->tok_value == INT_LITERAL)
			{
				if (strlen(cur->next->tok_string) != 14)
				{
					rc = INVALID_TIMESTAMP;
					cur->next->tok_value = INVALID;
				}
				else
				{
					if (cur->next->next->tok_value == EOC)
					{
						struct stat buffer2;
						tpd_entry * cur_tab = &(g_tpd_list->tpd_start);

						if ((stat("db.log", &buffer2) == 0))
						{
							fp = fopen("dbfile.bin", "wb");
							g_tpd_list->db_flags = 0;
							fwrite(g_tpd_list, g_tpd_list->list_size, 1, fp);
							fflush(fp);
							fclose(fp);
							fp = NULL;
							rename("db.log", "db.old");
							fp = fopen("db.old", "r");
							aux = fopen("db.log", "w");
							buffer = (char *)calloc(500, sizeof(char));
							tolog = (char *)calloc(500, sizeof(char));
							temp_char = (char *)calloc(20, sizeof(char));
							date = (char *)calloc(15, sizeof(char));
							memcpy(date, cur->next->tok_string, 14);
							date[14] = '\0';
							rtime1 = getTime(date);
							while (fgets(buffer, 500, fp) != NULL)
							{
								memcpy(tolog, buffer, 500);
								token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;
								if (found == true)
								{
									memcpy(temp_char, buffer, 10);
									temp_char[6] = '\0';
									if (strcmp(temp_char, "BACKUP\0") != 0)
									{
										memcpy(temp_char, buffer, 20);
										temp_char[14] = '\0';
										rtime2 = getTime(temp_char);
										if (difftime(rtime1, rtime2) >= 0)
										{
											fprintf(aux, "%s", tolog);
											buffer[strlen(buffer) - 2] = '\0';
											get_token((buffer + 16), &tok_list);
											do_semantic(tok_list);
										}
										else
										{
											prune = true;
											break;
										}
									}
								}
								if (found == false)
								{
									memcpy(temp_char, buffer, 20);
									temp_char[8] = '\0';
									if (strcmp(temp_char, "RF_START\0") == 0)
									{
										found = true;
										g_tpd_list->db_flags = ROLLING_FORWARD;
										fgets(buffer, 500, fp);
										memcpy(tolog, buffer, 500);
										memcpy(temp_char, buffer, 10);
										temp_char[6] = '\0';
										if (strcmp(temp_char, "BACKUP\0") != 0)
										{
											memcpy(temp_char, buffer, 20);
											temp_char[14] = '\0';
											rtime2 = getTime(temp_char);
											if (difftime(rtime1, rtime2) >= 0)
											{
												fprintf(aux, "%s", tolog);
												buffer[strlen(buffer) - 2] = '\0';
												get_token((buffer + 16), &tok_list);
												do_semantic(tok_list);
											}
											else
											{
												prune = true;
												break;
											}
										}
									}
									else
									{
										fprintf(aux, "%s", tolog);
									}
									tok_ptr = tok_list;
									while (tok_ptr != NULL)
									{
										tmp_tok_ptr = tok_ptr->next;
										free(tok_ptr);
										tok_ptr = tmp_tok_ptr;
									}
								}

							}
							if (found == false)
							{
								rc = RF_START_MISSING;
								cur->tok_value = INVALID;
							}
							fflush(fp);
							fclose(fp);
							fp = NULL;
							fflush(aux);
							fclose(aux);
							aux = NULL;

						}
						else
						{
							rc = LOG_MISSING;
							cur->tok_value = INVALID;
						}
						free(buffer);
						free(temp_char);
						free(date);
					}
					else
					{
						rc = TOO_MANY_ARGUMENTS;
						cur->next->next->tok_value = INVALID;
					}
				}
			}
			else
			{
				rc = INVALID;
				cur->next->tok_value = INVALID;
			}
		}
		else
		{
			rc = MISSING_ARGUMENTS;
			cur->tok_value = INVALID;
		}
	}
	else
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	if (prune == true)
	{
		int k = 1;
		char number[5];
		char *file_name = (char *)calloc(10, sizeof(char));
		strcat(file_name, "db.log");
		//		itoa(k, number, 10);
		sprintf(number, "%d", k);
		strcat(file_name, number);

		struct stat buffer2;
		tpd_entry * cur_tab = &(g_tpd_list->tpd_start);

		while ((stat(file_name, &buffer2) == 0))
		{
			k++;
			free(file_name);
			file_name = (char *)calloc(10, sizeof(char));
			strcat(file_name, "db.log");
			sprintf(number, "%d", k);
			//			itoa(k, number, 10);
			strcat(file_name, number);
		}
		rename("db.old", file_name);
		struct _stat st;
		FILE *remov = fopen(file_name, "rb");
		_fstat(_fileno(remov), &st);
		char *readed = (char *)calloc(st.st_size, sizeof(char));
		fread(readed, 1, st.st_size, remov);
		fclose(remov);
		size_t newSize = deleteLine(readed, st.st_size, "RF_START");
		remov = fopen(file_name, "wb");
		fwrite(readed, 1, newSize, remov);
		fclose(remov);
		free(readed);
		free(file_name);
	}
	return rc;
}

time_t getTime(char *szYYYYMMDDHHMMSS)
{
	struct tm    Tm;
	memset(&Tm, 0, sizeof(Tm));
	char buffer[5];
	/*YEAR*/
	memcpy(buffer, szYYYYMMDDHHMMSS, sizeof(char) * 4);
	buffer[4] = '\0';
	szYYYYMMDDHHMMSS += 4;
	Tm.tm_year = atoi(buffer) - 1900;
	/*MON*/
	memcpy(buffer, szYYYYMMDDHHMMSS, sizeof(char) * 2);
	buffer[2] = '\0';
	szYYYYMMDDHHMMSS += 2;
	Tm.tm_mon = atoi(buffer) - 1;
	/*Day*/
	memcpy(buffer, szYYYYMMDDHHMMSS, sizeof(char) * 2);
	buffer[2] = '\0';
	szYYYYMMDDHHMMSS += 2;
	Tm.tm_mday = atoi(buffer);
	/*HOUR*/
	memcpy(buffer, szYYYYMMDDHHMMSS, sizeof(char) * 2);
	buffer[2] = '\0';
	szYYYYMMDDHHMMSS += 2;
	Tm.tm_hour = atoi(buffer);
	/*MIN*/
	memcpy(buffer, szYYYYMMDDHHMMSS, sizeof(char) * 2);
	buffer[2] = '\0';
	szYYYYMMDDHHMMSS += 2;
	Tm.tm_min = atoi(buffer);
	/*SEC*/
	memcpy(buffer, szYYYYMMDDHHMMSS, sizeof(char) * 2);
	buffer[2] = '\0';
	szYYYYMMDDHHMMSS += 2;
	Tm.tm_sec = atoi(buffer);
	return mktime(&Tm);
}

size_t deleteLine( char* buffer, size_t size, const char* line )
{
  char* p = buffer; 
  bool done = false;
  size_t len = strlen(line);
  size_t newSize = 0;
  do
  {
    char* q = strchr( p, *line );
    if ( q != NULL )
    {
      if ( strncmp( q, line, len ) == 0 )
      {
        size_t lineSize = 1; 
        for ( char* line = q; *line != '\n'; ++line) 
        {
          ++lineSize;
        }
        size_t restSize = (size_t)((buffer + size) - (q + lineSize));
        memmove( q, q + lineSize, restSize );
        newSize = size - lineSize;
        done = true;
      }
      else
      {
        p = q + 1; // continue search
      }
    }
    else
    {
      done = true;
    }
  }
  while (!done);

  return newSize;
}