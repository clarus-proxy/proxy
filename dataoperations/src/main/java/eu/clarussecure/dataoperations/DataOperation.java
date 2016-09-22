package eu.clarussecure.dataoperations;

/**
 * CLARUS Data Operation module interface.
 */
public interface DataOperation {
    /** Outbound GET operation.
     * @param attributeNames names of the attributes, as given in the security policy for the current dataset.
     * @param criteria conditions of the get call, in the same order as the attributeNames.
     * @param operation operation to perform on the result (AVERAGE, MAX, MIN...)
     * @return an Promise object with a reference to the call and original parameters.
     */
    public Promise get(String[] attributeNames, String[] criteria, Operation operation);

    /** Inbound GET operation (RESPONSE), reconstructs data received by CSP.
     * @param promise reference to the original call
     * @param contents data returned by the CSP.
     * @returns the unprotected data (if applicable)
     */
    public String[][] get(Promise promise, String[][] contents);

    /** Outbound POST Operation, modifies data according to security policy.
     * @param attributeNames names of the attributes, as given in the security policy for the current dataset.
     * @param contents unprotected records
     * @returns protected data, using the same structure as provided in contents.
     */
    public String[][] post(String[] attributeNames, String[][] contents);

    /** Outbound PUT Operation, modifies data specified by criteria, according to security policy.
     * @param attributeNames names of the attributes, as given in the security policy for the current dataset.
     * @param criteria conditions of the get call, in the same order as the attributeNames.
     * @param contents unprotected records
     * @returns protected data, using the same structure as provided in contents.
     */
    public String[][] put(String[] attributeNames, String[] criteria, String[][] contents);

    /** Outbound DELETE Operation, deletes data specified by criteria.
     * @param attributeNames names of the attributes, as given in the security policy for the current dataset.
     * @param criteria conditions of the get call, in the same order as the attributeNames.
     */
    public void delete(String[] attributeNames, String[] criteria);
}
