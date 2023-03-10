//
//  tpcc_workload.cc
//

#include "tpcc_workload.h"

void TPCCWorkload::init(ycsbc::TransactionalSplinterDB *db) {
	_db = db;
	// load all tables in the database
	tpcc_buffer = new drand48_data * [g_num_wh];
	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	for (uint32_t i = 0; i < g_num_wh; i++) {
		thread_args a = {this, i};
		pthread_create(&p_thds[i], NULL, thread_init_tables, &a);
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
		uint64_t key = iKey(r.i_id);
		_db->Store(&key, &r, sizeof(r));
	}
}

void TPCCWorkload::init_warehouse_table(uint64_t wid) {
	printf("Init warehouse : %d\n", wid);
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
	uint64_t key = wKey(r.w_id);
	_db->Store(&key, &r, sizeof(r));
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
		uint64_t key = dKey(did, wid);
		_db->Store(&key, &r, sizeof(r));
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
		uint64_t key = sKey(sid, wid);
		_db->Store(&key, &r, sizeof(r));
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
		// aaasz: store only by primary key
		uint64_t key = cKey(cid, did, wid);
		_db->Store(&key, &r, sizeof(r));
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
		uint64_t key = oKey(wid, did, oid);
		_db->Store(&key, &r, sizeof(r));

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
			uint64_t key = olKey(wid, did, oid);
			_db->Store(&key, &r, sizeof(r));
		}

		// NEW ORDER
		if (oid > 2100) {
			tpcc_new_order_row_t no_r;
			no_r.no_o_id = oid;
			no_r.no_d_id = did;
			no_r.no_w_id = wid;
			uint64_t key = noKey(cid, did, wid);
			_db->Store(&key, &r, sizeof(r));
		}
	}
}

void TPCCTransaction::init(uint64_t thd_id) {
   double x = (double)(rand() % 100) / 100.0;
   if (x < g_perc_payment)
      gen_payment(thd_id);
   else 
      gen_new_order(thd_id);
}

void TPCCTransaction::gen_payment(uint64_t thd_id) {
	type = TPCC_PAYMENT;
	//if (FIRST_PART_LOCAL)
	//	w_id = thd_id % g_num_wh + 1;
	//else
		w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	// d_w_id = w_id;
	// uint64_t part_id = wh_to_part(w_id);
	// part_to_access[0] = part_id;
	// part_num = 1;

	// d_id = URand(1, DIST_PER_WARE, w_id-1);
	// h_amount = URand(1, 5000, w_id-1);
	// int x = URand(1, 100, w_id-1);
	// int y = URand(1, 100, w_id-1);


	// if(x <= 85) { 
	// 	// home warehouse
	// 	c_d_id = d_id;
	// 	c_w_id = w_id;
	// } else {	
	// 	// remote warehouse
	// 	c_d_id = URand(1, DIST_PER_WARE, w_id-1);
	// 	if(g_num_wh > 1) {
	// 		while((c_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
	// 		if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
	// 			part_to_access[1] = wh_to_part(c_w_id);
	// 			part_num = 2;
	// 		}
	// 	} else 
	// 		c_w_id = w_id;
	// }
	// if(y <= 60) {
	// 	// by last name
	// 	by_last_name = true;
	// 	Lastname(NURand(255,0,999,w_id-1),c_last);
	// } else {
	// 	// by cust id
	// 	by_last_name = false;
	// 	c_id = NURand(1023, 1, g_cust_per_dist,w_id-1);
	// }
}

void TPCCTransaction::gen_new_order(uint64_t thd_id) {
	// type = TPCC_NEW_ORDER;
	// if (FIRST_PART_LOCAL)
	// 	w_id = thd_id % g_num_wh + 1;
	// else
	// 	w_id = URand(1, g_num_wh, thd_id % g_num_wh);
	// d_id = URand(1, DIST_PER_WARE, w_id-1);
	// c_id = NURand(1023, 1, g_cust_per_dist, w_id-1);
	// rbk = URand(1, 100, w_id-1);
	// ol_cnt = URand(5, 15, w_id-1);
	// o_entry_d = 2013;
	// items = (Item_no *) _mm_malloc(sizeof(Item_no) * ol_cnt, 64);
	// remote = false;
	// part_to_access[0] = wh_to_part(w_id);
	// part_num = 1;

	// for (UInt32 oid = 0; oid < ol_cnt; oid ++) {
	// 	items[oid].ol_i_id = NURand(8191, 1, g_max_items, w_id-1);
	// 	UInt32 x = URand(1, 100, w_id-1);
	// 	if (x > 1 || g_num_wh == 1)
	// 		items[oid].ol_supply_w_id = w_id;
	// 	else  {
	// 		while((items[oid].ol_supply_w_id = URand(1, g_num_wh, w_id-1)) == w_id) {}
	// 		remote = true;
	// 	}
	// 	items[oid].ol_quantity = URand(1, 10, w_id-1);
	// }
	// // Remove duplicate items
	// for (UInt32 i = 0; i < ol_cnt; i ++) {
	// 	for (UInt32 j = 0; j < i; j++) {
	// 		if (items[i].ol_i_id == items[j].ol_i_id) {
	// 			for (UInt32 k = i; k < ol_cnt - 1; k++)
	// 				items[k] = items[k + 1];
	// 			ol_cnt --;
	// 			i--;
	// 		}
	// 	}
	// }
	// for (UInt32 i = 0; i < ol_cnt; i ++) 
	// 	for (UInt32 j = 0; j < i; j++) 
	// 		assert(items[i].ol_i_id != items[j].ol_i_id);
	// // update part_to_access
	// for (UInt32 i = 0; i < ol_cnt; i ++) {
	// 	UInt32 j;
	// 	for (j = 0; j < part_num; j++ ) 
	// 		if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
	// 			break;
	// 	if (j == part_num) // not found! add to it.
	// 	part_to_access[part_num ++] = wh_to_part( items[i].ol_supply_w_id );
	// }
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

bool TPCCTransaction::run() {
	switch (type) {
		case TPCC_PAYMENT :
			return run_payment(); break;
		case TPCC_NEW_ORDER :
			return run_new_order(); break;
		default:
			assert(false);
	}
}

bool TPCCTransaction::run_payment() {
// 	RC rc = RCOK;
// 	uint64_t key;
// 	itemid_t * item;

// 	uint64_t w_id = query->w_id;
//     uint64_t c_w_id = query->c_w_id;
// 	/*====================================================+
//     	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
// 		WHERE w_id=:w_id;
// 	+====================================================*/
// 	/*===================================================================+
// 		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
// 		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
// 		FROM warehouse
// 		WHERE w_id=:w_id;
// 	+===================================================================*/

// 	// TODO for variable length variable (string). Should store the size of 
// 	// the variable.
// 	key = query->w_id;
// 	INDEX * index = _wl->i_warehouse; 
// 	item = index_read(index, key, wh_to_part(w_id));
// 	assert(item != NULL);
// 	row_t * r_wh = ((row_t *)item->location);
// 	row_t * r_wh_local;
// 	if (g_wh_update)
// 		r_wh_local = get_row(r_wh, WR);
// 	else 
// 		r_wh_local = get_row(r_wh, RD);

// 	if (r_wh_local == NULL) {
// 		return finish(Abort);
// 	}
// 	double w_ytd;
	
// 	r_wh_local->get_value(W_YTD, w_ytd);
// 	if (g_wh_update) {
// 		r_wh_local->set_value(W_YTD, w_ytd + query->h_amount);
// 	}
// 	char w_name[11];
// 	char * tmp_str = r_wh_local->get_value(W_NAME);
// 	memcpy(w_name, tmp_str, 10);
// 	w_name[10] = '\0';
// 	/*=====================================================+
// 		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
// 		WHERE d_w_id=:w_id AND d_id=:d_id;
// 	+=====================================================*/
// 	key = distKey(query->d_id, query->d_w_id);
// 	item = index_read(_wl->i_district, key, wh_to_part(w_id));
// 	assert(item != NULL);
// 	row_t * r_dist = ((row_t *)item->location);
// 	row_t * r_dist_local = get_row(r_dist, WR);
// 	if (r_dist_local == NULL) {
// 		return finish(Abort);
// 	}

// 	double d_ytd;
// 	r_dist_local->get_value(D_YTD, d_ytd);
// 	r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
// 	char d_name[11];
// 	tmp_str = r_dist_local->get_value(D_NAME);
// 	memcpy(d_name, tmp_str, 10);
// 	d_name[10] = '\0';

// 	/*====================================================================+
// 		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
// 		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
// 		FROM district
// 		WHERE d_w_id=:w_id AND d_id=:d_id;
// 	+====================================================================*/

// 	row_t * r_cust;
// 	if (query->by_last_name) { 
// 		/*==========================================================+
// 			EXEC SQL SELECT count(c_id) INTO :namecnt
// 			FROM customer
// 			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
// 		+==========================================================*/
// 		/*==========================================================================+
// 			EXEC SQL DECLARE c_byname CURSOR FOR
// 			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
// 			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
// 			FROM customer
// 			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
// 			ORDER BY c_first;
// 			EXEC SQL OPEN c_byname;
// 		+===========================================================================*/

// 		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
// 		// XXX: the list is not sorted. But let's assume it's sorted... 
// 		// The performance won't be much different.
// 		INDEX * index = _wl->i_customer_last;
// 		item = index_read(index, key, wh_to_part(c_w_id));
// 		assert(item != NULL);
		
// 		int cnt = 0;
// 		itemid_t * it = item;
// 		itemid_t * mid = item;
// 		while (it != NULL) {
// 			cnt ++;
// 			it = it->next;
// 			if (cnt % 2 == 0)
// 				mid = mid->next;
// 		}
// 		r_cust = ((row_t *)mid->location);
		
// 		/*============================================================================+
// 			for (n=0; n<namecnt/2; n++) {
// 				EXEC SQL FETCH c_byname
// 				INTO :c_first, :c_middle, :c_id,
// 					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
// 					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
// 			}
// 			EXEC SQL CLOSE c_byname;
// 		+=============================================================================*/
// 		// XXX: we don't retrieve all the info, just the tuple we are interested in
// 	}
// 	else { // search customers by cust_id
// 		/*=====================================================================+
// 			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
// 			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
// 			c_discount, c_balance, c_since
// 			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
// 			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
// 			:c_discount, :c_balance, :c_since
// 			FROM customer
// 			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
// 		+======================================================================*/
// 		key = custKey(query->c_id, query->c_d_id, query->c_w_id);
// 		INDEX * index = _wl->i_customer_id;
// 		item = index_read(index, key, wh_to_part(c_w_id));
// 		assert(item != NULL);
// 		r_cust = (row_t *) item->location;
// 	}

//   	/*======================================================================+
// 	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
//    		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
//    	+======================================================================*/
// 	row_t * r_cust_local = get_row(r_cust, WR);
// 	if (r_cust_local == NULL) {
// 		return finish(Abort);
// 	}
// 	double c_balance;
// 	double c_ytd_payment;
// 	double c_payment_cnt;

// 	r_cust_local->get_value(C_BALANCE, c_balance);
// 	r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
// 	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
// 	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
// 	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
// 	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

// 	char * c_credit = r_cust_local->get_value(C_CREDIT);

// 	if ( strstr(c_credit, "BC") ) {
	
// 		/*=====================================================+
// 		    EXEC SQL SELECT c_data
// 			INTO :c_data
// 			FROM customer
// 			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
// 		+=====================================================*/
// //	  	char c_new_data[501];
// //	  	sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f",
// //	      	c_id, c_d_id, c_w_id, d_id, w_id, query->h_amount);
// //		char * c_data = r_cust->get_value("C_DATA");
// //	  	strncat(c_new_data, c_data, 500 - strlen(c_new_data));
// //		r_cust->set_value("C_DATA", c_new_data);
			
// 	}
	
// 	char h_data[25];
// _Pragma("GCC diagnostic push")
// _Pragma("GCC diagnostic ignored \"-Wstringop-truncation\"")
//         strncpy(h_data, w_name, 10);
// _Pragma("GCC diagnostic pop")
// 	int length = strlen(h_data);
// 	if (length > 10) length = 10;
// 	strcpy(&h_data[length], "    ");
// 	strncpy(&h_data[length + 4], d_name, 10);
// 	h_data[length+14] = '\0';
// 	/*=============================================================================+
// 	  EXEC SQL INSERT INTO
// 	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
// 	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
// 	  +=============================================================================*/
// //	row_t * r_hist;
// //	uint64_t row_id;
// //	_wl->t_history->get_new_row(r_hist, 0, row_id);
// //	r_hist->set_value(H_C_ID, c_id);
// //	r_hist->set_value(H_C_D_ID, c_d_id);
// //	r_hist->set_value(H_C_W_ID, c_w_id);
// //	r_hist->set_value(H_D_ID, d_id);
// //	r_hist->set_value(H_W_ID, w_id);
// //	int64_t date = 2013;		
// //	r_hist->set_value(H_DATE, date);
// //	r_hist->set_value(H_AMOUNT, h_amount);
// #if !TPCC_SMALL
// //	r_hist->set_value(H_DATA, h_data);
// #endif
// //	insert_row(r_hist, _wl->t_history);

// 	assert( rc == RCOK );
// 	return finish(rc);
}

bool TPCCTransaction::run_new_order() {
// 	RC rc = RCOK;
// 	uint64_t key;
// 	itemid_t * item;
// 	INDEX * index;
	
// 	bool remote = query->remote;
// 	uint64_t w_id = query->w_id;
//     uint64_t d_id = query->d_id;
//     uint64_t c_id = query->c_id;
// 	uint64_t ol_cnt = query->ol_cnt;
// 	/*=======================================================================+
// 	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
// 		INTO :c_discount, :c_last, :c_credit, :w_tax
// 		FROM customer, warehouse
// 		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
// 	+========================================================================*/
// 	key = w_id;
// 	index = _wl->i_warehouse; 
// 	item = index_read(index, key, wh_to_part(w_id));
// 	assert(item != NULL);
// 	row_t * r_wh = ((row_t *)item->location);
// 	row_t * r_wh_local = get_row(r_wh, RD);
// 	if (r_wh_local == NULL) {
// 		return finish(Abort);
// 	}


// 	double w_tax;
// 	r_wh_local->get_value(W_TAX, w_tax); 
// 	key = custKey(c_id, d_id, w_id);
// 	index = _wl->i_customer_id;
// 	item = index_read(index, key, wh_to_part(w_id));
// 	assert(item != NULL);
// 	row_t * r_cust = (row_t *) item->location;
// 	row_t * r_cust_local = get_row(r_cust, RD);
// 	if (r_cust_local == NULL) {
// 		return finish(Abort); 
// 	}
// 	uint64_t c_discount;
// 	//char * c_last;
// 	//char * c_credit;
// 	r_cust_local->get_value(C_DISCOUNT, c_discount);
// 	//c_last = r_cust_local->get_value(C_LAST);
// 	//c_credit = r_cust_local->get_value(C_CREDIT);
 	
// 	/*==================================================+
// 	EXEC SQL SELECT d_next_o_id, d_tax
// 		INTO :d_next_o_id, :d_tax
// 		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
// 	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
// 		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
// 	+===================================================*/
// 	key = distKey(d_id, w_id);
// 	item = index_read(_wl->i_district, key, wh_to_part(w_id));
// 	assert(item != NULL);
// 	row_t * r_dist = ((row_t *)item->location);
// 	row_t * r_dist_local = get_row(r_dist, WR);
// 	if (r_dist_local == NULL) {
// 		return finish(Abort);
// 	}
// 	//double d_tax;
// 	int64_t o_id;
// 	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
// 	o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
// 	o_id ++;
// 	r_dist_local->set_value(D_NEXT_O_ID, o_id);

// 	/*========================================================================================+
// 	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
// 		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
// 	+========================================================================================*/
// //	row_t * r_order;
// //	uint64_t row_id;
// //	_wl->t_order->get_new_row(r_order, 0, row_id);
// //	r_order->set_value(O_ID, o_id);
// //	r_order->set_value(O_C_ID, c_id);
// //	r_order->set_value(O_D_ID, d_id);
// //	r_order->set_value(O_W_ID, w_id);
// //	r_order->set_value(O_ENTRY_D, o_entry_d);
// //	r_order->set_value(O_OL_CNT, ol_cnt);
// //	int64_t all_local = (remote? 0 : 1);
// //	r_order->set_value(O_ALL_LOCAL, all_local);
// //	insert_row(r_order, _wl->t_order);
// 	/*=======================================================+
//     EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
//         VALUES (:o_id, :d_id, :w_id);
//     +=======================================================*/
// //	row_t * r_no;
// //	_wl->t_neworder->get_new_row(r_no, 0, row_id);
// //	r_no->set_value(NO_O_ID, o_id);
// //	r_no->set_value(NO_D_ID, d_id);
// //	r_no->set_value(NO_W_ID, w_id);
// //	insert_row(r_no, _wl->t_neworder);
// 	for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {

// 		uint64_t ol_i_id = query->items[ol_number].ol_i_id;
// 		uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
// 		uint64_t ol_quantity = query->items[ol_number].ol_quantity;
// 		/*===========================================+
// 		EXEC SQL SELECT i_price, i_name , i_data
// 			INTO :i_price, :i_name, :i_data
// 			FROM item
// 			WHERE i_id = :ol_i_id;
// 		+===========================================*/
// 		key = ol_i_id;
// 		item = index_read(_wl->i_item, key, 0);
// 		assert(item != NULL);
// 		row_t * r_item = ((row_t *)item->location);

// 		row_t * r_item_local = get_row(r_item, RD);
// 		if (r_item_local == NULL) {
// 			return finish(Abort);
// 		}
// 		int64_t i_price;
// 		//char * i_name;
// 		//char * i_data;
// 		r_item_local->get_value(I_PRICE, i_price);
// 		//i_name = r_item_local->get_value(I_NAME);
// 		//i_data = r_item_local->get_value(I_DATA);

// 		/*===================================================================+
// 		EXEC SQL SELECT s_quantity, s_data,
// 				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
// 				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
// 			INTO :s_quantity, :s_data,
// 				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
// 				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
// 			FROM stock
// 			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
// 		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
// 			WHERE s_i_id = :ol_i_id
// 			AND s_w_id = :ol_supply_w_id;
// 		+===============================================*/

// 		uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
// 		INDEX * stock_index = _wl->i_stock;
// 		itemid_t * stock_item;
// 		index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
// 		assert(item != NULL);
// 		row_t * r_stock = ((row_t *)stock_item->location);
// 		row_t * r_stock_local = get_row(r_stock, WR);
// 		if (r_stock_local == NULL) {
// 			return finish(Abort);
// 		}
		
// 		// XXX s_dist_xx are not retrieved.
// 		UInt64 s_quantity;
// 		int64_t s_remote_cnt;
// 		s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
// #if !TPCC_SMALL
// 		int64_t s_ytd;
// 		int64_t s_order_cnt;
// 		//char * s_data = "test";
// 		r_stock_local->get_value(S_YTD, s_ytd);
// 		r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
// 		r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
// 		r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
// 		//s_data = r_stock_local->get_value(S_DATA);
// #endif
// 		if (remote) {
// 			s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
// 			s_remote_cnt ++;
// 			r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
// 		}
// 		uint64_t quantity;
// 		if (s_quantity > ol_quantity + 10) {
// 			quantity = s_quantity - ol_quantity;
// 		} else {
// 			quantity = s_quantity - ol_quantity + 91;
// 		}
// 		r_stock_local->set_value(S_QUANTITY, &quantity);

// 		/*====================================================+
// 		EXEC SQL INSERT
// 			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
// 				ol_i_id, ol_supply_w_id,
// 				ol_quantity, ol_amount, ol_dist_info)
// 			VALUES(:o_id, :d_id, :w_id, :ol_number,
// 				:ol_i_id, :ol_supply_w_id,
// 				:ol_quantity, :ol_amount, :ol_dist_info);
// 		+====================================================*/
// 		// XXX district info is not inserted.
// //		row_t * r_ol;
// //		uint64_t row_id;
// //		_wl->t_orderline->get_new_row(r_ol, 0, row_id);
// //		r_ol->set_value(OL_O_ID, &o_id);
// //		r_ol->set_value(OL_D_ID, &d_id);
// //		r_ol->set_value(OL_W_ID, &w_id);
// //		r_ol->set_value(OL_NUMBER, &ol_number);
// //		r_ol->set_value(OL_I_ID, &ol_i_id);
// #if !TPCC_SMALL
// //		int w_tax=1, d_tax=1;
// //		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
// //		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
// //		r_ol->set_value(OL_QUANTITY, &ol_quantity);
// //		r_ol->set_value(OL_AMOUNT, &ol_amount);
// #endif		
// //		insert_row(r_ol, _wl->t_orderline);
// 	}
// 	assert( rc == RCOK );
// 	return finish(rc);
}


