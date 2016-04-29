package br.edu.ifba.gsort.hadoop.stackoverflow.MapReduce.job2;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class UserXPost implements Comparable<UserXPost> {

	private String id;
	private String displayName;
	private Integer postCount;
	
	

	public UserXPost(String id, String displayName, Integer postCount) {
		super();
		this.id = id;
		this.displayName = displayName;
		this.postCount = postCount;
	}
	


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public Integer getPostCount() {
		return postCount;
	}

	public void setPostCount(Integer postCount) {
		this.postCount = postCount;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((displayName == null) ? 0 : displayName.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((postCount == null) ? 0 : postCount.hashCode());
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserXPost other = (UserXPost) obj;
		if (displayName == null) {
			if (other.displayName != null)
				return false;
		} else if (!displayName.equals(other.displayName))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (postCount == null) {
			if (other.postCount != null)
				return false;
		} else if (!postCount.equals(other.postCount))
			return false;
		return true;
	}



	@Override
	public int compareTo(UserXPost arg0) {
		int retorno = arg0.getPostCount().compareTo(this.getPostCount());
		if (retorno == 0) {
			return arg0.getId().compareTo(this.getId());
		} else {
			return retorno;
		}
	}


	@Override
	public String toString() {
		return "UserXPost [id=" + id + ", displayName=" + displayName + ", postCount=" + postCount + "]";
	}
	
	

}
