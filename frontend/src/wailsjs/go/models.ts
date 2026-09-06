export namespace broker {
	
	export class DeleteResult {
	    deleted_files_count: number;
	    freed_bytes: number;
	    freed_bytes_formatted: string;
	    remaining_bytes: number;
	    remaining_bytes_formatted: string;
	    message: string;
	
	    static createFrom(source: any = {}) {
	        return new DeleteResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.deleted_files_count = source["deleted_files_count"];
	        this.freed_bytes = source["freed_bytes"];
	        this.freed_bytes_formatted = source["freed_bytes_formatted"];
	        this.remaining_bytes = source["remaining_bytes"];
	        this.remaining_bytes_formatted = source["remaining_bytes_formatted"];
	        this.message = source["message"];
	    }
	}
	export class LogStorageStats {
	    total_size_bytes: number;
	    total_size_formatted: string;
	    max_size_bytes: number;
	    max_size_formatted: string;
	    usage_percent: number;
	    is_near_threshold: boolean;
	    global_log_count: number;
	    job_log_count: number;
	    oldest_log_date: string;
	    newest_log_date: string;
	    recent_entries: string[];
	
	    static createFrom(source: any = {}) {
	        return new LogStorageStats(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.total_size_bytes = source["total_size_bytes"];
	        this.total_size_formatted = source["total_size_formatted"];
	        this.max_size_bytes = source["max_size_bytes"];
	        this.max_size_formatted = source["max_size_formatted"];
	        this.usage_percent = source["usage_percent"];
	        this.is_near_threshold = source["is_near_threshold"];
	        this.global_log_count = source["global_log_count"];
	        this.job_log_count = source["job_log_count"];
	        this.oldest_log_date = source["oldest_log_date"];
	        this.newest_log_date = source["newest_log_date"];
	        this.recent_entries = source["recent_entries"];
	    }
	}

}

export namespace main {
	
	export class JobSummary {
	    id: string;
	    status: string;
	    progress: number;
	    source_file: string;
	    source_title: string;
	    created_at: string;
	    timestamp: number;
	    total_tasks: number;
	    done_tasks: number;
	    has_media: boolean;
	    can_audit_asr: boolean;
	    can_audit_nmt: boolean;
	
	    static createFrom(source: any = {}) {
	        return new JobSummary(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.status = source["status"];
	        this.progress = source["progress"];
	        this.source_file = source["source_file"];
	        this.source_title = source["source_title"];
	        this.created_at = source["created_at"];
	        this.timestamp = source["timestamp"];
	        this.total_tasks = source["total_tasks"];
	        this.done_tasks = source["done_tasks"];
	        this.has_media = source["has_media"];
	        this.can_audit_asr = source["can_audit_asr"];
	        this.can_audit_nmt = source["can_audit_nmt"];
	    }
	}

}

