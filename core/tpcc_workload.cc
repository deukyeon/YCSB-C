//
//  tpcc_workload.cc
//

#include "tpcc_workload.h"

using namespace tpcc;

void TPCCTransaction::init(uint64_t thd_id) {
   double x = (double)(rand() % 100) / 100.0;
   if (x < g_perc_payment)
      gen_payment(thd_id);
   else
      gen_new_order(thd_id);
}

void TPCCTransaction::gen_payment(uint64_t thd_id) {
	type = TPCC_PAYMENT;
	w_id = thd_id % g_num_wh + 1;
	d_w_id = w_id;
	d_id = URand(1, DIST_PER_WARE, w_id-1);
	h_amount = URand(1, 5000, w_id-1);
	int x = URand(1, 100, w_id-1);

	if(x <= 85) {
		// home warehouse
		c_d_id = d_id;
		c_w_id = w_id;
	} else {
		// remote warehouse
		c_d_id = URand(1, DIST_PER_WARE, w_id-1);
		if(g_num_wh > 1) {
			while((c_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
		} else
			c_w_id = w_id;
	}
	// aaasz: we just lookup clients by their id
	c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
}

void TPCCTransaction::gen_new_order(uint64_t thd_id) {
	type = TPCC_NEW_ORDER;
	w_id = thd_id % g_num_wh + 1;
	d_id = URand(1, DIST_PER_WARE, w_id-1);
	c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
	rbk = URand(1, 100, w_id-1);
	ol_cnt = URand(5, 15, w_id-1);
	o_entry_d = 2023;
	items = (item_no *) _mm_malloc(sizeof(item_no) * ol_cnt, 64);
	remote = false;
	// part_to_access[0] = wh_to_part(w_id);
	// part_num = 1;

	for (uint32_t oid = 0; oid < ol_cnt; oid ++) {
		items[oid].ol_i_id = NURand(8191, 1, g_max_items, w_id-1);
		uint32_t x = URand(1, 100, w_id-1);
		if (x > 1 || g_num_wh == 1)
			items[oid].ol_supply_w_id = w_id;
		else  {
			while((items[oid].ol_supply_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
			remote = true;
		}
		items[oid].ol_quantity = URand(1, 10, w_id-1);
	}
	// Remove duplicate items
	// for (uint32_t i = 0; i < ol_cnt; i ++) {
	// 	for (uint32_t j = 0; j < i; j++) {
	// 		if (items[i].ol_i_id == items[j].ol_i_id) {
	// 			for (uint32_t k = i; k < ol_cnt - 1; k++)
	// 				items[k] = items[k + 1];
	// 			ol_cnt --;
	// 			i--;
	// 		}
	// 	}
	// }

	// for (uint32_t i = 0; i < ol_cnt; i ++)
	// 	for (uint32_t j = 0; j < i; j++)
	// 		assert(items[i].ol_i_id != items[j].ol_i_id);
}

void
TPCCTransaction::gen_order_status(uint64_t thd_id) {
	// type = TPCC_ORDER_STATUS;
	// if (FIRST_PART_LOCAL)
	// 	w_id = thd_id % g_num_wh + 1;
	// else
	// 	w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	// d_id = URand(1, DIST_PER_WARE, w_id-1);
	// c_w_id = w_id;
	// c_d_id = d_id;
	// int y = URand(1, 100, w_id-1);
	// if(y <= 60) {
	// 	// by last name
	// 	by_last_name = true;
	// 	Lastname(NURand(255,0,999,w_id-1),c_last);
	// } else {
	// 	// by cust id
	// 	by_last_name = false;
	// 	c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
	// }
}

void TPCCWorkload::init(ycsbc::TransactionalSplinterDB *db) {
	printf("Initializing TPCCWorkload; num_wh = %d, g_max_items = %d, g_cust_per_dist = %d, g_max_txn_retry = %d, g_abort_penalty_us = %d\n", g_num_wh, g_max_items, g_cust_per_dist, g_max_txn_retry, g_abort_penalty_us);
	_db = db;
	// load all tables in the database
	tpcc_buffer = new drand48_data * [g_num_wh];
	pthread_t p_thds[g_num_wh];
	thread_args a[g_num_wh];
	for (uint32_t i = 0; i < g_num_wh; i++) {
		a[i] = {.wl = this, .tid = i};
		pthread_create(&p_thds[i], NULL, thread_init_tables, &a[i]);
	}
	for (uint32_t i = 0; i < g_num_wh; i++) 
		pthread_join(p_thds[i], NULL);

	printf("TPCC Data Initialization Complete!\n");
}

void *TPCCWorkload::thread_init_tables(void *args) {
	TPCCWorkload::thread_args * a = (TPCCWorkload::thread_args *) args;
	a->wl->_db->Init();
	uint32_t wid = a->tid + 1;
	tpcc_buffer[a->tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	assert((uint64_t)a->tid < g_num_wh);
	srand48_r(wid, tpcc_buffer[a->tid]);
	
	if (a->tid == 0)
		a->wl->init_item_table();
	a->wl->init_warehouse_table(wid);
	a->wl->init_district_table(wid);
	a->wl->init_stock_table(wid);
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
	 	a->wl->init_customer_table(did, wid);
	 	a->wl->init_order_table(did, wid);
	}
	a->wl->_db->Close();
	return NULL;
}

void TPCCWorkload::init_item_table() { 
	tpcc_item_row_t r;
	for (uint32_t i = 1; i <= g_max_items; i++) {
		r.i_id = i;
		r.i_im_id = URand(1L,10000L, 0);
		MakeAlphaString(14, 24, r.i_name, 0);
		r.i_price = URand(1, 100, 0);
		MakeAlphaString(26, 50, r.i_data, 0);
		// TODO in TPCC, "original" should start at a random position
		if (Rand(10, 0) == 0) 
			strcpy(r.i_data, "original");
		TPCCKey key = iKey(r.i_id);
		_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));
	}
}

void TPCCWorkload::init_warehouse_table(uint64_t wid) {
	assert(wid >= 1 && wid <= g_num_wh);
	tpcc_warehouse_row_t r;
	r.w_id = wid;
	MakeAlphaString(6, 10, r.w_name, wid-1);
    MakeAlphaString(10, 20, r.w_street_1, wid-1);
    MakeAlphaString(10, 20, r.w_street_2, wid-1);
    MakeAlphaString(10, 20, r.w_city, wid-1);
	MakeAlphaString(2, 2, r.w_state, wid-1); /* State */
   	MakeNumberString(9, 9, r.w_zip, wid-1); /* Zip */
   	r.w_tax = (double)URand(0L,200L,wid-1)/1000.0;
   	r.w_ytd=300000.00;
	TPCCKey key = wKey(r.w_id);
	_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));
}

void TPCCWorkload::init_district_table(uint64_t wid) {
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		tpcc_district_row_t r;
		r.d_id = did;
		r.d_w_id = wid;
		MakeAlphaString(6, 10, r.d_name, wid-1);
        MakeAlphaString(10, 20, r.d_street_1, wid-1);
        MakeAlphaString(10, 20, r.d_street_2, wid-1);
        MakeAlphaString(10, 20, r.d_city, wid-1);
		MakeAlphaString(2, 2, r.d_state, wid-1); /* State */
    	MakeNumberString(9, 9, r.d_zip, wid-1); /* Zip */
    	r.d_tax = (double)URand(0L,200L,wid-1)/1000.0;
    	r.d_ytd=30000.00;
		r.d_next_o_id = 3001;
		TPCCKey key = dKey(did, wid);
		_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));
	}
}

void TPCCWorkload::init_stock_table(uint64_t wid) {
	for (uint32_t sid = 1; sid <= g_max_items; sid++) {
		tpcc_stock_row_t r;
		r.s_i_id = sid;
		r.s_w_id = wid;
		r.s_quantity = URand(10, 100, wid-1);
		r.s_remote_cnt = 0;
		MakeAlphaString(24, 24, r.s_dist_01, wid-1);
		MakeAlphaString(24, 24, r.s_dist_02, wid-1);
		MakeAlphaString(24, 24, r.s_dist_03, wid-1);
		MakeAlphaString(24, 24, r.s_dist_04, wid-1);
		MakeAlphaString(24, 24, r.s_dist_05, wid-1);
		MakeAlphaString(24, 24, r.s_dist_06, wid-1);
		MakeAlphaString(24, 24, r.s_dist_07, wid-1);
		MakeAlphaString(24, 24, r.s_dist_08, wid-1);
		MakeAlphaString(24, 24, r.s_dist_09, wid-1);
		MakeAlphaString(24, 24, r.s_dist_10, wid-1);
		r.s_ytd = 0;
		r.s_order_cnt = 0;
		int len = MakeAlphaString(26, 50, r.s_data, wid-1);	
		if (rand() % 100 < 10) {
			int idx = URand(0, len - 8, wid-1);
			strcpy(&r.s_data[idx], "original");
		}
		TPCCKey key = sKey(sid, wid);
		_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));
	}
}

void TPCCWorkload::init_customer_table(uint64_t did, uint64_t wid) {
	assert(g_cust_per_dist >= 1000);
	for (uint32_t cid = 1; cid <= g_cust_per_dist; cid++) {
		tpcc_customer_row_t r;
		r.c_id = cid;
		r.c_d_id = did;
		r.c_w_id = wid;
		if (cid <= 1000)
			Lastname(cid - 1, r.c_last);
		else
			Lastname(NURand(255,0,999,wid-1), r.c_last);
		memcpy(r.c_middle, "OE", 2);
		MakeAlphaString(FIRSTNAME_MINLEN, FIRSTNAME_LEN, r.c_first, wid-1);
        MakeAlphaString(10, 20, r.c_street_1, wid-1);
        MakeAlphaString(10, 20, r.c_street_2, wid-1);
        MakeAlphaString(10, 20, r.c_city, wid-1);
		MakeAlphaString(2, 2, r.c_state, wid-1); /* State */
    	MakeNumberString(9, 9, r.c_zip, wid-1); /* Zip */
  		MakeNumberString(16, 16, r.c_phone, wid-1); /* Zip */
		r.c_since = 0;
		r.c_credit_lim = 50000;
		r.c_delivery_cnt = 0;
        MakeAlphaString(300, 500, r.c_data, wid-1);
		if (Rand(10, wid-1) == 0) {
			memcpy(r.c_credit, "GC", 2);
		} else {
			memcpy(r.c_credit, "BC", 2);
		}
		r.c_discount = (double)Rand(5000,wid-1) / 10000;
		r.c_balance = -10.0;
		r.c_ytd_payment = 10.0;
		r.c_payment_cnt = 1;
		// aaasz: we store only by primary key
		TPCCKey key = cKey(cid, did, wid);
		_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));
	}
}

void 
TPCCWorkload::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
	uint32_t i;
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) 
		perm_c_id[i] = i+1;

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
}

void TPCCWorkload::init_order_table(uint64_t did, uint64_t wid) {
	uint64_t perm[g_cust_per_dist]; 
	init_permutation(perm, wid); /* initialize permutation of customer numbers */
	for (uint32_t oid = 1; oid <= g_cust_per_dist; oid++) {
		tpcc_order_row_t r;
		uint64_t o_ol_cnt = 1;
		uint64_t cid = perm[oid - 1]; //get_permutation();
		r.o_id = oid;
		r.o_c_id = cid;
		r.o_d_id = did;
		r.o_w_id = wid;
		uint64_t o_entry = 2013;
		r.o_entry_d = o_entry;
		if (oid < 2101)
			r.o_carrier_id = URand(1, 10, wid-1);
		else
			r.o_carrier_id = 0;
		o_ol_cnt = URand(5, 15, wid-1);
		r.o_ol_cnt = o_ol_cnt;
		r.o_all_local = 1;
		TPCCKey key = oKey(wid, did, oid);
		_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));

		// ORDER-LINE	
		for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
			tpcc_order_line_row_t ol_r;
			ol_r.ol_o_id = oid;
			ol_r.ol_d_id = did;
			ol_r.ol_w_id = wid;
			ol_r.ol_number = ol;
			ol_r.ol_i_id = URand(1, 100000, wid-1);
			ol_r.ol_supply_w_id = wid;
			if (oid < 2101) {
				ol_r.ol_delivery_d = o_entry;
				ol_r.ol_amount = (double)URand(1, 999999, wid-1)/100;
			} else {
				ol_r.ol_delivery_d = 0;
				ol_r.ol_amount = (double)URand(1, 999999, wid-1)/100;
			}
			ol_r.ol_quantity = 5;
			MakeAlphaString(24, 24, ol_r.ol_dist_info, wid-1);
			TPCCKey key = olKey(wid, did, oid, ol);
			_db->Store(&key, sizeof(TPCCKey), &r, sizeof(r));
		}

		// NEW ORDER
		if (oid > 2100) {
			tpcc_new_order_row_t no_r;
			no_r.no_o_id = oid;
			no_r.no_d_id = did;
			no_r.no_w_id = wid;
			TPCCKey key = noKey(oid, did, wid);
			_db->Store(&key, sizeof(TPCCKey), &no_r, sizeof(r));
		}
	}
}


int TPCCWorkload::run_transaction(TPCCTransaction *txn) {
	switch (txn->type) {
		case TPCC_PAYMENT :
			return run_payment(txn);
		case TPCC_NEW_ORDER :
			return run_new_order(txn);
		default:
			assert(false);
	}
}

int TPCCWorkload::run_payment(TPCCTransaction *txn) {
	TPCCKey key;
	tpcc_warehouse_row_t w_r;
	tpcc_district_row_t d_r;
	tpcc_customer_row_t c_r;
	tpcc_history_row_t h_r;

	//printf("PAYMENT on warehouse: %lu, district: %lu, client: %lu\n", txn->w_id, txn->d_id, txn->c_id);

	ycsbc::Transaction *t = NULL;
    _db->Begin(&t);

	// increase the YTD amount of the home warehouse
	key = wKey(txn->w_id);
	_db->Read(t, &key, sizeof(TPCCKey), &w_r, sizeof(w_r));
	w_r.w_ytd += txn->h_amount;
	_db->Update(t, &key, sizeof(TPCCKey), &w_r, sizeof(w_r));

	// increase the YTD amount of the district of the home warehouse
	key = dKey(txn->d_id, txn->w_id);
	_db->Read(t, &key, sizeof(TPCCKey), &d_r, sizeof(d_r));
	d_r.d_ytd += txn->h_amount;
	_db->Update(t, &key, sizeof(TPCCKey), &d_r, sizeof(d_r));

	// update the customer info
	key = cKey(txn->c_id, txn->c_d_id, txn->c_w_id);
	_db->Read(t, &key, sizeof(TPCCKey), &c_r, sizeof(c_r));
	c_r.c_balance -= txn->h_amount;
	c_r.c_ytd_payment += txn->h_amount;
	c_r.c_payment_cnt += 1;

	if (!strncmp(c_r.c_credit, "BC", 2)) {
		char c_new_data[501];
		sprintf(c_new_data,"| %4ld %2ld %4ld %2ld %4ld $%7.2f",
				txn->c_id, txn->c_d_id, txn->c_w_id, txn->d_id, txn->w_id, txn->h_amount);
		strncat(c_new_data, c_r.c_data, 500 - strlen(c_new_data));
		memcpy(c_r.c_data, c_new_data, 500);
	}

	_db->Update(t, &key, sizeof(TPCCKey), &c_r, sizeof(c_r));

	// insert row into history
	key = hKey(txn->c_id, txn->c_d_id, txn->c_w_id);
	h_r.h_amount = txn->h_amount;
	h_r.h_date = 2023;
	h_r.h_c_id = txn->c_id;
	h_r.h_c_d_id = txn->c_d_id;
	h_r.h_c_w_id = txn->c_w_id;
	h_r.h_d_id = txn->d_id;
	h_r.h_w_id = txn->w_id;

	memcpy(h_r.h_data, w_r.w_name, 10);
	memcpy(h_r.h_data + 10, "    ", 4);
	memcpy(h_r.h_data + 14, d_r.d_name, 10);
	_db->Update(t, &key, sizeof(TPCCKey), &h_r, sizeof(h_r));

	return _db->Commit(&t);
}

int TPCCWorkload::run_new_order(TPCCTransaction *txn) {
	TPCCKey key;
	tpcc_warehouse_row_t w_r;
	tpcc_district_row_t d_r;
	tpcc_customer_row_t c_r;
	tpcc_stock_row_t s_r;
	tpcc_order_row_t o_r;
	tpcc_new_order_row_t no_r;
	tpcc_order_line_row_t ol_r;
	tpcc_item_row_t i_r;
	uint64_t order_id;

	double total_amount = 0; // transaction's output displayed at the terminal
	//char brand_generic[txn->ol_cnt]; // transaction's output displayed at the terminal

	//printf("NEW_ORDER on warehouse: %lu, district: %lu, client: %lu\n", txn->w_id, txn->d_id, txn->c_id);

	ycsbc::Transaction *t = NULL;
    _db->Begin(&t);

	// get warehouse info
	key = wKey(txn->w_id);
	_db->Read(t, &key, sizeof(TPCCKey), &w_r, sizeof(w_r));
	key = dKey(txn->d_id, txn->w_id);

	// get district info and bump the next order id
	_db->Read(t, &key, sizeof(TPCCKey), &d_r, sizeof(d_r));
	order_id = d_r.d_next_o_id + 1;
	d_r.d_next_o_id = order_id;
	_db->Update(t, &key, sizeof(TPCCKey), &d_r, sizeof(d_r));

	// get customer info
	key = cKey(txn->c_id, txn->c_d_id, txn->c_w_id);
	_db->Read(t, &key, sizeof(TPCCKey), &c_r, sizeof(c_r));

	// insert into NEW_ORDER
	key = noKey(txn->w_id, txn->d_id, order_id);
	no_r.no_w_id = txn->w_id;
	no_r.no_d_id = txn->d_id;
	no_r.no_o_id = order_id;
	_db->Update(t, &key, sizeof(TPCCKey), &no_r, sizeof(no_r));

	// insert into ORDER
	key = oKey(txn->w_id, txn->d_id, order_id);
	o_r.o_id = order_id;
	o_r.o_c_id = txn->c_id;
	o_r.o_d_id = txn->d_id;
	o_r.o_w_id = txn->w_id;
	o_r.o_entry_d = txn->o_entry_d;
	o_r.o_ol_cnt = txn->ol_cnt;
	o_r.o_all_local = (txn->remote? 0 : 1);
	_db->Update(t, &key, sizeof(TPCCKey), &o_r, sizeof(o_r));

	// insert into ORDER_LINE and STOCK
	ol_r.ol_i_id = order_id;
	ol_r.ol_w_id = txn->w_id;
	ol_r.ol_d_id = txn->d_id;
	for (uint32_t ol_number = 0; ol_number < txn->ol_cnt; ol_number++) {
		ol_r.ol_i_id = txn->items[ol_number].ol_i_id;
		ol_r.ol_supply_w_id = txn->items[ol_number].ol_supply_w_id;
		ol_r.ol_quantity = txn->items[ol_number].ol_quantity;
		ol_r.ol_number = ol_number;

		// get item info
		key = iKey(ol_r.ol_i_id);
		_db->Read(t, &key, sizeof(TPCCKey), &i_r, sizeof(i_r));

		// get and update stock info
		key = sKey(ol_r.ol_i_id, ol_r.ol_supply_w_id);
		_db->Read(t, &key, sizeof(TPCCKey), &s_r, sizeof(s_r));
		if (s_r.s_quantity >= ol_r.ol_quantity + 10) {
			s_r.s_quantity = s_r.s_quantity - ol_r.ol_quantity;
		} else {
			s_r.s_quantity = s_r.s_quantity - ol_r.ol_quantity + 91;
		}
		s_r.s_ytd += ol_r.ol_quantity;
		s_r.s_order_cnt += 1;
		if (ol_r.ol_supply_w_id != txn->w_id) {
			s_r.s_remote_cnt += 1;
		}
		_db->Update(t, &key, sizeof(TPCCKey), &s_r, sizeof(s_r));

		ol_r.ol_amount = ol_r.ol_quantity * i_r.i_price;
		total_amount += ol_r.ol_amount;

		// TODO: compute the brand_generic bits
		//  if 'original' in i_r.i_data and 'original' in i_r.s_data
        //         brand_generic[ol_number] = 'B'
        // else
        //         brand_generic[ol_number] = 'G'

		key = olKey(txn->w_id, txn->d_id, order_id, ol_number);
		_db->Update(t, &key, sizeof(TPCCKey), &ol_r, sizeof(ol_r));
	}

	total_amount = total_amount * (1 + w_r.w_tax + d_r.d_tax) * (1 - c_r.c_discount);

	// TODO: do we want to use rbk?

	return _db->Commit(&t);;
}



